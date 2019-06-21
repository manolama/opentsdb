package net.opentsdb.query.processor.downsample;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.NoDataPartialTimeSeriesPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.utils.DateTime;

/**
 * A segment of downsample data. This is an ugly class so check out how it works:
 * <p>
 * There are two modes:
 * <ul>
 * <li>Single source set per DS Set.</li>
 * <li>Multiple source sets pre DS Set.</li>
 * </ul>
 * <p>
 * The single case is pretty straight forward. E.g. if we have 1m downsampling
 * and 1 hour source segments then we would create 1h DS segments and every
 * time series would be able to downsample within a segment. In this case we
 * increment the #count for every PTS received. When we get the total time series
 * count from the source set and the source set is complete, we'll tag this set
 * as complete.
 * <p>
 * Multiple sources may be needed in cases such as:
 * <ul>
 * <li>Calendaring is in effect with a DST zone that breaks alignment.</li>
 * <li>Odd intervals are used that would span multiple sources.</li>
 * <li>Run-all is enabled.</li>
 * </ul>
 * <p>
 * In this case we have to keep track of NoDataPartialTimeSeries and real series.
 * We use the time series map, set boundaries array, etc to track our series.
 */
public class DownsamplePartialTimeSeriesSet implements PartialTimeSeriesSet,
   TimeSpecification, CloseablePooledObject {
  private static final Logger LOG = LoggerFactory.getLogger(
      DownsamplePartialTimeSeriesSet.class);
  
  protected final static long END_MASK = 0x00000000FFFFFFFFL;
  
  protected PooledObject pooled_object;
  
  protected Downsample node;
  protected TimeStamp start = new SecondTimeStamp(0L);
  protected TimeStamp end = new SecondTimeStamp(0L);
  protected int total_sets;
  protected String source;
  protected long interval;
  
  // format [32b start ts epoch:32b end ts epoch][series counts]...
  // also our sentinel to tell if multi's are enabled or not.
  protected AtomicLongArray set_boundaries;
  
  // TODO - padding which means unwrapping the arrays and atomic intervals so 
  // they are no longer object references.
  protected AtomicBoolean[] completed_array;
  
  protected AtomicBoolean all_sets_accounted_for = new AtomicBoolean();
  protected AtomicBoolean complete = new AtomicBoolean();
  protected AtomicInteger count = new AtomicInteger();
  protected List<NoDataPartialTimeSeries> ndptss = Lists.newArrayList();
  protected volatile int last_multi;
  protected int array_size;
  
  // TODO - different data types
  protected Map<Long, DownsamplePartialTimeSeries> timeseries = Maps.newConcurrentMap();
  
  void reset(final Downsample node, final String source, final int idx) {
    this.node = node;
    this.source = source;
    final long[] sizes = node.getSizes(source);
    interval = sizes[1];
    total_sets = (int) sizes[2];
    start.updateEpoch(sizes[idx + 3]);
    
    if (idx + 4 >= sizes.length) {
      end.update(start);
      if (((DownsampleConfig) node.config()).timezone() != null) {
        end.add(DateTime.durationFromSeconds(interval / 1000));
      } else {
        end.add(Duration.ofMillis(interval));
      }
      
      if ((end.epoch() - (sizes[0] / 1000)) > 
        node.pipelineContext().query().endTime().epoch()) {
        System.out.println("   USING the query time for end.");
        end.update(((DownsampleConfig) node.config()).endTime());
      }
    } else {
      end.updateEpoch(sizes[idx + 4]);
    }
    
    array_size = (int) ((end.epoch() - start.epoch()) / (sizes[0] / 1000));
  }
  
  void process(final PartialTimeSeries series) {
    if (complete.get()) {
      // this can happen if for some reason the upstream node sent us a set
      // that's outside of the query bounds when we're on the tail set and it
      // overlaps the query end by one or more intervals.
      if (series.set().start().compare(Op.GTE, end)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received and dropping a PTS for segment " 
              + series.set().start().epoch() + " to " 
              + series.set().end().epoch() + " which was beyond our "
              + "query end time of " + end.epoch());
        }
      } else {
        LOG.warn("Received and dropping a PTS for segment " 
            + series.set().start().epoch() + " to " 
            + series.set().end().epoch() + " which was within our "
            + "query end time of " + end.epoch() + ", but we were complete.");
      }
      return;
    }
    
    boolean multiples = !(series.set().start().compare(Op.LTE, start) &&
        series.set().end().compare(Op.GTE, end));
    System.out.println(" [ds]  MD: " + (series.set().end().msEpoch() - 
        series.set().start().msEpoch()) + "  INT: " + node.interval_ms);

    // This is where it gets really really ugly as we may need multiple sets 
    // in this single set and we need to track what we've seen and haven't seen.
    // So we'll keep the start and end time of each set in a single long (as two
    // ints) and the final time series count in the adjacent index so we can
    // track how many series we expect.
    if (multiples) {
      System.out.println("******* MULTIPLES *******  Working series sent to set of class: " + series.getClass());
      handleMultiples(series);
    } else if (series instanceof NoDataPartialTimeSeries) {
      System.out.println("  WORKING NDPTS");
      if (complete.compareAndSet(false, true)) {
        // TODO return no data
        final NoDataPartialTimeSeries ndpts = (NoDataPartialTimeSeries)
          node.pipelineContext().tsdb().getRegistry().getObjectPool(
              NoDataPartialTimeSeriesPool.TYPE).claim().object();
        ndpts.reset(this);
        node.sendUpstream(ndpts);
      } else {
        LOG.warn("Received a No Data PTS after sending upstream.");
      }
    } else {
      final int count = this.count.incrementAndGet();
      if (series.set().complete() && count == series.set().timeSeriesCount()) {
        System.out.println("  SINGLES are complete!");
        if (!complete.compareAndSet(false, true)) {
          LOG.warn("Singles set as complete after being marked complete. "
              + "Programming error.");
        }
      }
      
      DownsampleNumericPartialTimeSeries pts;
      if (series.value().type() == NumericLongArrayType.TYPE) {
        pts = (DownsampleNumericPartialTimeSeries) node.pipelineContext().tsdb().getRegistry().getObjectPool(DownsampleNumericPartialTimeSeriesPool.TYPE).claim().object();
        pts.reset(this);
        pts.addSeries(series);
      } else {
        throw new RuntimeException("Unhandled type: " + series.value().type());
      }
    }
  }

  @Override
  public void close() throws Exception {
    start.updateEpoch(-1);
    end.updateEpoch(-1);
    set_boundaries = null;
    completed_array = null;
    all_sets_accounted_for.set(false);
    complete.set(false);
    count.set(0);
    ndptss.clear();
    last_multi = 0;
    timeseries.clear();
    release();
  }

  @Override
  public int totalSets() {
    return total_sets;
  }

  @Override
  public boolean complete() {
    return complete.get();
  }

  @Override
  public QueryNode node() {
    return node;
  }

  @Override
  public String dataSource() {
    return source;
  }

  @Override
  public TimeStamp start() {
    return start;
  }

  @Override
  public TimeStamp end() {
    return end;
  }
  
  @Override
  public int timeSeriesCount() {
    return count.get();
  }

  @Override
  public TimeSpecification timeSpecification() {
    return this;
  }
  
  @Override
  public TemporalAmount interval() {
    return ((DownsampleConfig) node.config()).interval();
  }

  @Override
  public String stringInterval() {
    return ((DownsampleConfig) node.config()).getInterval();
  }

  @Override
  public ChronoUnit units() {
    return ((DownsampleConfig) node.config()).units();
  }

  @Override
  public ZoneId timezone() {
    return ((DownsampleConfig) node.config()).timezone();
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    for (int i = 0; i < offset; i++) {
      timestamp.add(((DownsampleConfig) node.config()).interval());
    }
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    timestamp.add(((DownsampleConfig) node.config()).interval());
  }
  
  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }

  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
  protected int arraySize() {
    return array_size;
  }
  
  protected void handleMultiples(final PartialTimeSeries series) {
    System.out.println("[[SET]] Incoming set: " + series.set().start().epoch() + " => " + series.set().end().epoch() + "  Local: " + this.start.epoch() + " => " + this.end.epoch());
    
    if (series.set().start().compare(Op.GTE, end)) {
      // edge case wherein we got a set outside of our boundary.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received and dropping a PTS for segment " 
            + series.set().start().epoch() + " to " 
            + series.set().end().epoch() + " which was beyond our "
            + "query end time of " + end.epoch());
      }
      return;
    }
    
    long start = 0;
    int boundary_index = -1;    
    // ugly walk but should be fairly quick as we don't expect arrays to be
    // too large.
    // TODO - determine if we need a better lookup.
    if (all_sets_accounted_for.get()) {
      System.out.println(" [[[ds]]] all sets in for multiples!!");
      // no need for sync since we're just incrementing counters at this point.
      checkMultipleComplete();
      
      for (int i = 0; i < set_boundaries.length(); i += 2) {
        start = set_boundaries.get(i) >>> 32;
        if (series.set().start().epoch() == start) {
          setMultiples(i, series);
          break;
        }
      }
      return;
    }
    
    // NOTE: We have to sync here as we shuffle indices around. *sniff*
    synchronized (this) {
      if (set_boundaries == null) {
        long concat = series.set().start().epoch() << 32;
        concat |= ((int) series.set().end().epoch());
        // use the min so we don't have to resize.
        final long[] sizes = node.getSizes(source);
        int size = (int) ((end.msEpoch() - this.start.msEpoch()) / sizes[0]);
        set_boundaries = new AtomicLongArray(size * 2);
        completed_array = new AtomicBoolean[size];
        for (int i = 0; i < size; i++) {
          completed_array[i] = new AtomicBoolean();
        }
        set_boundaries.set(0, concat);
        
        if (size == 1) {
          System.out.println("ALL IN! for set " + this.start.epoch());
          all_sets_accounted_for.set(true);
        }
        
        setMultiples(0, series);
        last_multi = 1;
        // we don't need to check for all in OR complete yet because we know we're missing
        // at least one.
        return;
      } 
      
      // we lost a race so let's double check to see if another series from the
      // same set is in here now.
      if (set_boundaries != null) {
        for (int i = 0; i < set_boundaries.length(); i += 2) {
          start = set_boundaries.get(i) >>> 32;
          if (series.set().start().epoch() <= start) {
            boundary_index = i;
            break;
          }
        }
      }
      
      if (boundary_index >= 0 && series.set().start().epoch() == start) {
        // found it, so see if we're done. Just increment now.
        //setMultiples(boundary_index, series);
      } else {
        // not found so we add it.
        long concat = series.set().start().epoch() << 32;
        concat |= ((int) series.set().end().epoch());
        // we didn't find it so we need to update the boundaries array
        // init if needed
        if (boundary_index < 0) {
          // didn't find one later than the start time so use the last free slot
          System.out.println("Setting at last idx: " + last_multi);
          set_boundaries.set(last_multi * 2, concat);
          boundary_index = last_multi * 2;
          last_multi++;
        } else {
          // shift
          System.out.println("************** SHIFTING! *************");
          for (int i = set_boundaries.length() - 1; i >= boundary_index + 2; i--) {
            set_boundaries.set(i, set_boundaries.get(i - 2));
          }
          // also have to do it for the completes!!
          for (int i = completed_array.length - 1; i >= (boundary_index / 2) + 1; i--) {
            completed_array[i].set(completed_array[i - 1].get());
          }
          
          // insert
          set_boundaries.set(boundary_index, concat);
          set_boundaries.set(boundary_index + 1, 0);      // reset post shift.
          completed_array[boundary_index / 2].set(false); // reset post shift.
          last_multi++;
        }
          
        // now we need to see if all sets have been seen.
        // TODO - mix in with the above for speed but for now we can re-walk.
        boolean all_in = true;
        long temp = set_boundaries.get(0);
        long end = 0;
        if (this.start.epoch() < temp >>> 32) {
          // missing the first set
          System.out.println(" NOT FIRST...  " + this.start.epoch() + " => " + (temp >>> 32));
          all_in = false;
        }
        
        if (all_in) {
          end = temp & END_MASK;
          for (int i = 2; i < last_multi * 2; i += 2) {
            temp = set_boundaries.get(i);
            if (end != temp >>> 32) {
              // next segment start didn't match current segments end.
              System.out.println("MISSING MIDD: " + i  + "  " + end + " => " + (temp >>> 32));
              all_in = false;
            }
            end = temp & END_MASK;
          }
        }
        
        if (all_in) {
          end = set_boundaries.get((last_multi - 1) * 2) & END_MASK;
          if (this.end.epoch() > end) {
            // missing the last segment.
            System.out.println("NOT END  " + this.end.epoch() + " => " + end +
                "  E-S " + (this.end.epoch() - this.start.epoch()) + "  D: " + 
                (this.end.epoch() - end));
            all_in = false;
          }
        }
        
        if (all_in) {
          // yay all in!
          System.out.println("ALL IN! for set " + this.start.epoch());
          all_sets_accounted_for.set(true);
          checkMultipleComplete();
        }
      }
    }
    
    // release the lock on the boundaries.
    setMultiples(boundary_index, series);
  }
  
  protected void setMultiples(final int index, final PartialTimeSeries series) {
    // TODO sync
    if (series instanceof NoDataPartialTimeSeries) {
      // TODO - gotta mark some series as done
      ndptss.add((NoDataPartialTimeSeries) series);
      for (DownsamplePartialTimeSeries pts : timeseries.values()) {
        pts.addSeries(series);
      }
      
      completed_array[index / 2].set(true);
      checkMultipleComplete();
      
      System.out.println("[[ SET ]] ndpts, checking complete: " + complete.get() + " and count " + count.get());
      if (complete.get() && count.get() == 0) {
        // send upstream empty.
        final NoDataPartialTimeSeries new_ndpts = (NoDataPartialTimeSeries) node.pipelineContext().tsdb().getRegistry().getObjectPool(NoDataPartialTimeSeriesPool.TYPE).claim().object();
        new_ndpts.reset(this);
        System.out.println("[[ SET ]] NDPTS done, sending up.");
        node.sendUpstream(new_ndpts);
        ndptss.clear(); // TODO - close the ndptss
      }
      
    } else {
      //System.out.println("  [[ SET ]] working id: " + series.idHash());
      DownsamplePartialTimeSeries pts = timeseries.get(series.idHash());
      if (pts == null) {
        //System.out.println(" [[ds]] MISS, making new dpts " + series.idHash());
        if (series.value().type() == NumericLongArrayType.TYPE) {
          pts = (DownsampleNumericPartialTimeSeries) node.pipelineContext().tsdb().getRegistry().getObjectPool(DownsampleNumericPartialTimeSeriesPool.TYPE).claim().object();
          DownsamplePartialTimeSeries extant = timeseries.putIfAbsent(series.idHash(), pts);
          if (extant != null) {
            try {
              pts.close();
            } catch (Exception e) {
              LOG.warn("Whoops, couldn't close PTS?", e);
            }
            pts = extant;
          } else {
            pts.reset(this);
          }
          for (final NoDataPartialTimeSeries ndpts : ndptss) {
            pts.addSeries(ndpts);
          }
        } else {
          throw new RuntimeException("Unhandled type: " + series.value().type());
        }
      }

      long finished = set_boundaries.incrementAndGet(index + 1);
      System.out.println("[[[ Multi ]] finished: " + finished + "   SetComp: " + series.set().complete() + "  SetTS: " + series.set().timeSeriesCount() + " TS: " + series.set().start().epoch());
      if (series.set().complete() && finished == series.set().timeSeriesCount()) {
        completed_array[index / 2].set(true);
        checkMultipleComplete();
      }
      
      //System.out.println("  sent to pts...");
      // IMPORTANT: This has to happen *after* we've marked the set complete.
      pts.addSeries(series);
    }
  }

  protected int lastMulti() {
    return last_multi;
  }
  
  protected boolean allSetsAccountedFor() {
    return all_sets_accounted_for.get();
  }
  
  private void checkMultipleComplete() {
    if (!all_sets_accounted_for.get()) {
      return;
    }
    // TODO - watch for races here on last_multi and the completed array.
    for (int i = 0; i < last_multi; i++) {
      if (!completed_array[i].get()) {
        System.out.println("   NOT DONE AT: " + i);
        return;
      } else {
        System.out.println("       COMPLETE AT: " + i);
      }
    }
    
    // all done!
    count.set(timeseries.size());
    if (!complete.compareAndSet(false, true)) {
      LOG.error("WTF? Set " + this.start.epoch() + " was marked complete more than once!");
    } else {
      System.out.println("[[ SET ]] set " + this.start.epoch() + " COMPLETE!  TS size: " + timeseries.size() + "  Count: " + count.get());
    }
  }

}

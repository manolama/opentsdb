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
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.NoDataPartialTimeSeriesPool;
import net.opentsdb.query.QueryNode;
import net.opentsdb.utils.DateTime;

public class DownsamplePartialTimeSeriesSet implements PartialTimeSeriesSet,
 TimeSpecification {
  private static final Logger LOG = LoggerFactory.getLogger(
      DownsamplePartialTimeSeriesSet.class);
  
  protected final static long END_MASK = 0x00000000FFFFFFFFL;
  
  protected Downsample node;
  TimeStamp start = new SecondTimeStamp(0L);
  TimeStamp end = new SecondTimeStamp(0L);
  int total_sets;
  String source;
  long interval;
  int size;
  
  // TODO - padding
  // format [32b start ts epoch:32b end ts epoch][series counts]...
  // also our sentinel to tell if multi's are enabled or not.
  AtomicLongArray set_boundaries;
  AtomicBoolean[] completed_array;
  
  AtomicBoolean all_sets_accounted_for = new AtomicBoolean();
  AtomicBoolean complete = new AtomicBoolean();
  AtomicInteger count = new AtomicInteger();
  List<NoDataPartialTimeSeries> ndptss = Lists.newArrayList();
  volatile int last_multi;
  
  // TODO - different data types
  Map<Long, DownsamplePartialTimeSeries> timeseries = Maps.newConcurrentMap();
  
  void reset(final Downsample node, final String source, final int idx) {
    this.node = node;
    this.source = source;
    final long[] sizes = node.getSizes(source);
    interval = sizes[1];
    total_sets = (int) sizes[2];
    start.updateEpoch(sizes[idx + 3]);
    end.update(start);
    
    if (node.use_calendar) {
      // TODO - 
    } else {
      end.add(Duration.ofSeconds(sizes[1] / 1000));
    }
    
    size = (int) ((end.msEpoch() - start.msEpoch()) /
        DateTime.parseDuration(((DownsampleConfig) node.config()).getInterval()));
  }
  
  void process(final PartialTimeSeries series) {
    boolean multiples = (series.set().end().msEpoch() - 
        series.set().start().msEpoch()) < node.interval_ms;
    System.out.println(" [ds]  MD: " + (series.set().end().msEpoch() - 
        series.set().start().msEpoch()) + "  INT: " + node.interval_ms);
    if (multiples) {
      System.out.println("******* MULTIPLES *******");
    }
    // This is where it gets really really ugly as we may need multiple sets 
    // in this single set and we need to track what we've seen and haven't seen.
    // So we'll keep the start and end time of each set in a single long (as two
    // ints) and the final time series count in the adjacent index so we can
    // track how many series we expect.
    if (multiples) {
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
        pts = new DownsampleNumericPartialTimeSeries(node.pipelineContext().tsdb()); // TODO - pool
        pts.reset(this, true);
        pts.addSeries(series);
      } else {
        throw new RuntimeException("Unhandled type: " + series.value().type());
      }
    }
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
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

  protected void handleMultiples(final PartialTimeSeries series) {
    long start = 0;
    int boundary_index = -1;    
    // ugly walk but should be fairly quick as we don't expect arrays to be
    // too large.
    // TODO - determine if we need a better lookup.
    if (all_sets_accounted_for.get()) {
      System.out.println(" [[[ds]]] all in for multiples!!");
      // no need for sync since we're just incrementing counters at this point.
      for (int i = 0; i < set_boundaries.length(); i += 2) {
        start = set_boundaries.get(i) >>> 32;
        if (series.set().start().epoch() <= start) {
          setMultiples(i, series);
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
        setMultiples(0, series);
        last_multi = 1;
        // we don't need to check for all in yet because we know we're missing
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
        setMultiples(boundary_index, series);
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
          setMultiples(last_multi * 2, series);
          last_multi++;
        } else {
          // shift
          for (int i = set_boundaries.length() - 1; i >= boundary_index + 2; i--) {
            set_boundaries.set(i, set_boundaries.get(i - 2));
          }
          // also have to do it for the completes!!
          for (int i = completed_array.length - 1; i >= (boundary_index / 2) + 1; i--) {
            completed_array[i].set(completed_array[i - 1].get());
          }
          
          // insert
          set_boundaries.set(boundary_index, concat);
          set_boundaries.set(boundary_index + 1, 0);
          setMultiples(boundary_index, series);
          last_multi++;
        }
          
        // now we need to see if all sets have been seen.
        // TODO - mix in with the above for speed but for now we can re-walk.
        long temp = set_boundaries.get(0);
        if (this.start.epoch() < temp >>> 32) {
          // missing the first set
          System.out.println(" NOT FIRST...  " + this.start.epoch() + " => " + (temp >>> 32));
          return;
        }
        
        long end = temp & END_MASK;
        for (int i = 2; i < last_multi * 2; i += 2) {
          temp = set_boundaries.get(i);
          if (end != temp >>> 32) {
            // next segment start didn't match current segments end.
            System.out.println("MISSING MIDD: " + i  + "  " + end + " => " + (temp >>> 32));
            return;
          }
          end = temp & END_MASK;
        }
        
        end = set_boundaries.get((last_multi - 1) * 2) & END_MASK;
        if (this.end.epoch() > end) {
          // missing the last segment.
          System.out.println("NOT END  " + this.end.epoch() + " => " + end +
              "  E-S " + (this.end.epoch() - this.start.epoch()) + "  D: " + 
              (end - this.end.epoch()));
          return;
        }
        
        // yay all in!
        System.out.println("ALL IN!");
        all_sets_accounted_for.set(true);
        checkMultipleComplete();
      }
    }
  }
  
  void setMultiples(final int index, final PartialTimeSeries series) {
    // TODO sync
    if (series instanceof NoDataPartialTimeSeries) {
      // TODO - gotta mark some series as done
      System.out.println(" ------- TODO handle NDPTS");
      ndptss.add((NoDataPartialTimeSeries) series);
      for (DownsamplePartialTimeSeries pts : timeseries.values()) {
        pts.addSeries(series);
      }
    } else {
      DownsamplePartialTimeSeries pts = timeseries.get(series.idHash());
      if (pts == null) {
        System.out.println(" [[ds]] MISS, making new dpts " + series.idHash());
        if (series.value().type() == NumericLongArrayType.TYPE) {
          pts = new DownsampleNumericPartialTimeSeries(node.pipelineContext().tsdb()); // TODO - pool
          DownsamplePartialTimeSeries extant = timeseries.putIfAbsent(series.idHash(), pts);
          if (extant != null) {
            pts = extant;
          } else {
            pts.reset(this, true);
          }
          for (final NoDataPartialTimeSeries ndpts : ndptss) {
            pts.addSeries(ndpts);
          }
          timeseries.put(series.idHash(), pts);
        } else {
          throw new RuntimeException("Unhandled type: " + series.value().type());
        }
      } else {
        System.out.println("  [[ds]] HIT!! Had dpts " + series.idHash());
      }
      
      System.out.println("  sent to pts...");
      pts.addSeries(series);
    }
    
    if (series instanceof NoDataPartialTimeSeries || 
        series.set().complete() && series.set().timeSeriesCount() == 0) {
      completed_array[index / 2].set(true);
      checkMultipleComplete();
    } else {
      long finished = set_boundaries.incrementAndGet(index + 1);
      if (series.set().complete() && finished == series.set().timeSeriesCount()) {
        completed_array[index / 2].set(true);
        checkMultipleComplete();
      }
    }
  }
  
  void checkMultipleComplete() {
    if (!all_sets_accounted_for.get()) {
      return;
    }
    for (int i = 0; i < last_multi; i++) {
      if (!completed_array[i].get()) {
        System.out.println("   NOT DONE AT: " + i);
        return;
      }
    }
    // all done!
    long max = 0;
    for (int i = 1; i < set_boundaries.length(); i += 2) {
      if (set_boundaries.get(i) > max) {
        max = set_boundaries.get(i);
      }
    }
    count.set((int) max);
    complete.compareAndSet(false, true);
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
}

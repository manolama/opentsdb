package net.opentsdb.query.processor.downsample;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BasePartialTimeSeries;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.PooledObject;

public class DownsampleNumericPartialTimeSeries extends 
  BasePartialTimeSeries<NumericArrayType> 
    implements DownsamplePartialTimeSeries<NumericArrayType>, NumericArrayType {

  protected final TSDB tsdb;
  
  protected List<PartialTimeSeries> series_list;
  protected volatile int pts_count;
  protected Downsample node;
  protected int expected_count;
  protected PooledObject value_array;
  protected long[] long_array;
  protected double[] double_array;
  
  protected PooledObject accumulator_array;
  protected long[] accumulator_long_array;
  protected double[] accumulator_double_array;
  protected int accumulator_idx = 0;
  protected MutableNumericValue mdp = new MutableNumericValue();
  protected TimeStamp boundary = new ZonedNanoTimeStamp(-1, 0, Const.UTC);
  protected TimeStamp next = new ZonedNanoTimeStamp(-1, 0, Const.UTC);
  
  /** The current write index for array stores. */
  protected int write_idx;
  
  protected DownsampleNumericPartialTimeSeries(final TSDB tsdb) {
    super();
    this.tsdb = tsdb;
  }
  
  @Override
  public void reset(final DownsamplePartialTimeSeriesSet set) {
    this.set = set; // this is the new downsample set.
    series_list = Lists.newArrayList();
    node = (Downsample) set.node();
  }
  
  @Override
  public void addSeries(final PartialTimeSeries series) {
    System.out.println(" [[[[[ds]]]] got series");
    id_hash = series.idHash();
    id_type = series.idType();
    if (id_type == null) {
      id_type = series.idType();
    }
    
    if (((DownsamplePartialTimeSeriesSet) set).lastMulti() < 1) {
      // WOOT! Simple case where we just agg and send it up
      runSingle(series);
      System.out.println("       SENT UP");
      ((Downsample) set.node()).sendUpstream(this);
      return;
    } else {
      // UGG!!! We have to follow the complex and ugly multi-series-path
      runMulti(series);
    }
  }
  
  @Override
  public TimeSeriesDataType value() {
    return this;
  }

  @Override
  public void release() {
    if (value_array != null) {
      value_array.release();
    }
    
    // TODO reset reset of it
    baseRelease();
  }
  
  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return write_idx;
  }

  @Override
  public boolean isInteger() {
    return long_array != null;
  }

  @Override
  public long[] longArray() {
    return long_array;
  }

  @Override
  public double[] doubleArray() {
    return double_array;
  }
  
  void runSingle(final PartialTimeSeries series) {
    if (((DownsampleConfig) node.config()).getRunAll()) {
      boundary.update(set.end());
    } else {
      boundary.update(set.start());
      boundary.add(((DownsampleConfig) node.config()).interval());
    }
    
    aggSeries(series);
    
    // release resources
    releaseAndFill();
  }
  
  // NOTE the barrier here. We could reduce the time it's locked but we shouldn't
  // block threads very long and it may be a good idea to back things up a bit
  // here anyway.
  synchronized void runMulti(final PartialTimeSeries series) {
    System.out.println("    [[[ds]]] run multiple in PTS! ");
    if (next.epoch() < 0) {
      next.update(set.start());
      if (((DownsampleConfig) node.config()).getRunAll()) {
        boundary.update(set.end());
      } else {
        boundary.update(set.start());
        boundary.add(((DownsampleConfig) node.config()).interval());
        System.out.println("   STARTING AT: " + boundary.epoch());
      }
    }
    
    // start with the given pts
    PartialTimeSeries current_pts = series;
    int temp = 0;
    do {
      System.out.println("     START LOOP W: " + current_pts.set().start().epoch());
      boolean added = false;
      if (current_pts instanceof NoDataPartialTimeSeries) {
        if (current_pts.set().start().compare(Op.LTE, next)) {
          // run it!
          if (accumulator_idx <= 0) {
            fillTillNext();
          } else {
            runAccumulatorOrFill(current_pts.set().end().msEpoch());
          }
          next.update(current_pts.set().end());
          //runAccumulatorOrFill(next.epoch());
        } else {
          // buffer since we're missing a piece
          series_list.add(current_pts);
          added = true;
        }
      } else if (current_pts.set().start().compare(Op.LTE, next)) {
        //next.update(current_pts.set().end());
        aggSeries(current_pts);
        System.out.println("     BOUNDARY " + boundary.epoch() + " Vs Next " + next.epoch() + "  " + (next.epoch() - boundary.epoch()));
        if (accumulator_idx <= 0) {
          fillTillNext();
        }
      } else {
        series_list.add(current_pts);
        added = true;
      }
      
      if (added) {
        System.out.println("ADDED!!!!!");
        break;
      }
      
      // otherwise we may be able to advance and release some series.
      current_pts = null;
      for (int i = 0; i < series_list.size(); i++) {
        current_pts = series_list.get(i);
        if (current_pts != null && 
            next.compare(Op.EQ, current_pts.set().start())) {
          series_list.set(i, null);
          break;
        }
      }
      
      if (++temp >= 10) {
        break;
      }
    } while (current_pts != null);
    
    // determine if we send it up or not.
    pts_count++;
    System.out.println("   RAN multi. All in? " + ((DownsamplePartialTimeSeriesSet) set).allSetsAccountedFor() + "  Last multi: " + ((DownsamplePartialTimeSeriesSet) set).lastMulti() + "  Cnt: " + pts_count + " NEXT: " + next.epoch());
    if (((DownsamplePartialTimeSeriesSet) set).allSetsAccountedFor() &&
        pts_count == ((DownsamplePartialTimeSeriesSet) set).lastMulti()) {
      System.out.println("COMPLETING But... " + accumulator_idx + " CMP: " + (set.end().epoch() - next.epoch()));
      releaseAndFill();
      System.out.println("     ---------- DS ------ Send multi upstream!!!");
      ((Downsample) set.node()).sendUpstream(this);
    }
  }
  
  void addLocal(final long value) {
    if (accumulator_long_array == null) {
      if (accumulator_double_array.length <= accumulator_idx) {
        growDouble();
      }
      accumulator_double_array[accumulator_idx++] = value;
    } else {
      if (accumulator_idx >= accumulator_long_array.length) {
        growLong();
      }
      accumulator_long_array[accumulator_idx++] = value;
    }
  }
  
  void addLocal(final double value) {
    if (accumulator_double_array == null) {
      flipFlopLocalArray();
    }
    if (accumulator_double_array.length <= accumulator_idx) {
      growDouble();
    }
    accumulator_double_array[accumulator_idx++] = value;
  }
  
  void flipFlopLocalArray() {
    PooledObject new_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
    if (((double[]) new_array.object()).length <= accumulator_long_array.length) {
      // ugg the pool is too small.
      // TODO - get a size from the pool BEFORE we claim it.
      new_array = null;
      accumulator_double_array = new double[accumulator_long_array.length];
    } else {
      accumulator_double_array = (double[]) new_array.object();
    }
    for (int i = 0; i < write_idx; i++) {
      accumulator_double_array[i] = accumulator_long_array[i];
    }
    accumulator_long_array = null;
    if (accumulator_array != null) {
      accumulator_array.release();
    }
    accumulator_array = new_array;
  }
  
  void flipFlopMainArray() {
    PooledObject new_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
    if (((double[]) new_array.object()).length <= 
        (long_array != null ? long_array.length : ((DownsamplePartialTimeSeriesSet) set).arraySize())) {
      // ugg the pool is too small.
      // TODO - get a size from the pool BEFORE we claim it.
      new_array = null;
      double_array = new double[long_array.length];
    } else {
      double_array = (double[]) new_array.object();
    }
    for (int i = 0; i < write_idx; i++) {
      double_array[i] = long_array[i];
    }
    long_array = null;
    if (value_array != null) {
      value_array.release();
    }
    value_array = new_array;
  }

  void growLong() {
    PooledObject new_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
    if (((long[]) new_array.object()).length <= accumulator_idx) {
      // UGG pool is too small
      // TODO - get a size from the pool BEFORE we claim it.
      new_array = null;
      accumulator_long_array = Arrays.copyOf(accumulator_long_array, 
          accumulator_long_array.length + 32);
      if (accumulator_array != null) {
        accumulator_array.release();
      }
    } else {
      for (int i = 0; i < accumulator_idx; i++) {
        ((long[]) new_array.object())[i] = accumulator_long_array[i];
      }
      accumulator_long_array = (long[]) new_array.object();
      if (accumulator_array != null) {
        accumulator_array.release();
      }
      accumulator_array = new_array;
    }
  }
  
  void growDouble() {
    PooledObject new_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
    if (((double[]) new_array.object()).length <= accumulator_idx) {
      // UGG pool is too small
      // TODO - get a size from the pool BEFORE we claim it.
      new_array = null;
      accumulator_double_array = Arrays.copyOf(accumulator_double_array, 
          accumulator_double_array.length + 32);
      if (accumulator_array != null) {
        accumulator_array.release();
      }
    } else {
      for (int i = 0; i < accumulator_idx; i++) {
        ((double[]) new_array.object())[i] = accumulator_double_array[i];
      }
      accumulator_double_array = (double[]) new_array.object();
      if (accumulator_array != null) {
        accumulator_array.release();
      }
      accumulator_array = new_array;
    }
  }

  void runAccumulatorOrFill(final long ts) {
    if (ts == boundary.msEpoch())
      System.out.println("        ACCUMULATE[[[[[[ EQUALS!!!!!! ]]]]]]]]" );
    if (accumulator_idx > 0) {
      System.out.println("        Flushing: " + accumulator_idx);
      // TODO - agg and put
      if (accumulator_long_array != null) {
        node.aggregator().run(accumulator_long_array, 0, accumulator_idx, mdp);
        if (mdp.isInteger() && long_array != null) {
          long_array[write_idx++] = mdp.longValue();
        } else {
          if (double_array == null) {
            flipFlopMainArray();
          }
          double_array[write_idx++] = mdp.toDouble();
        }
        System.out.println("AGG: " + mdp);
      } else {
        node.aggregator().run(accumulator_double_array, 0, accumulator_idx, ((DownsampleConfig) node.config()).getInfectiousNan(), mdp);
        if (long_array != null && double_array == null) {
          flipFlopMainArray();
        }
        double_array[write_idx++] = mdp.doubleValue();
        System.out.println("AGG: " + mdp);
      }
      System.out.println("       [ds] ran: " + boundary.epoch());
      if (!((DownsampleConfig) node.config()).getRunAll()) {
        boundary.add(((DownsampleConfig) node.config()).interval());
      }
      accumulator_idx = 0;
    } else if (ts == boundary.msEpoch()) {
      // edge case wherein the timestamp matches the boundary but we hadn't 
      // written anything for the last cell.
      if (long_array != null && double_array == null) {
        flipFlopMainArray();
      } else if (double_array == null) {
        initDouble();
      }
      double_array[write_idx++] = Double.NaN;
      if (!((DownsampleConfig) node.config()).getRunAll()) {
        boundary.add(((DownsampleConfig) node.config()).interval());
      }
      return;
    }
    
    while (!((DownsampleConfig) node.config()).getRunAll() && 
        boundary.msEpoch() <= ts) {
      System.out.println("                 ok, filling here");
      // TODO - proper fill
      if (long_array != null && double_array == null) {
        flipFlopMainArray();
      } else if (double_array == null) {
        initDouble();
      }
      double_array[write_idx++] = Double.NaN;
      if (!((DownsampleConfig) node.config()).getRunAll()) {
        boundary.add(((DownsampleConfig) node.config()).interval());
      }
    }
  }

  void fillRemainder() {
    // if the write index is less than the size we need to fill
    while (write_idx < ((DownsamplePartialTimeSeriesSet) set).arraySize()) {
      // TODO other fills
      if (double_array == null) {
        flipFlopMainArray();
      }
      
      double_array[write_idx++] = Double.NaN;
    }
  }
  
  void fillTillNext() {
    while (boundary.compare(Op.LTE, next)) {
      // TODO other fills
      if (double_array == null) {
        flipFlopMainArray();
      }
      
      double_array[write_idx++] = Double.NaN;
      if (!((DownsampleConfig) node.config()).getRunAll()) {
        boundary.add(((DownsampleConfig) node.config()).interval());
      }
    }
  }

  void releaseAndFill() {
    if (accumulator_array != null) {
      accumulator_array.release();
      accumulator_array = null;
    }
    accumulator_long_array = null;
    accumulator_double_array = null;
    
    fillRemainder();
  }
  
  void aggSeries(final PartialTimeSeries series) {
    final long[] values = ((NumericLongArrayType) series.value()).data();
    int idx = ((NumericLongArrayType) series.value()).offset();
    
    if (((NumericLongArrayType) series.value()).end() > 
        ((NumericLongArrayType) series.value()).offset()) {
      ChronoUnit units = NumericLongArrayType.timestampUnits(values, idx);
      TimeStamp ts = (units == ChronoUnit.NANOS || 
          ((DownsampleConfig) node.config()).timezone() != Const.UTC) ?
              new MillisecondTimeStamp(0) : 
                new ZonedNanoTimeStamp(0, 0, ((DownsampleConfig) node.config()).timezone());
      
      while (idx <= ((NumericLongArrayType) series.value()).end()) {
        units = NumericLongArrayType.timestampUnits(values, idx);
        NumericLongArrayType.timestampInNanos(values, idx, ts);
        System.out.println("   (IDX) " + idx + "  DIF : " + (boundary.msEpoch() - ts.msEpoch()));
        // skip values earlier than our start time and those later than our end time
        if (ts.compare(Op.LT, set.start()) ||
            (next.epoch() > 0 && ts.compare(Op.LT, next))) {
          System.out.println("    DROPPING: " + (next.msEpoch() - ts.msEpoch()));
          // TODO - nanos
          idx += 2;
          continue;
        }
        
        // stop if we've reached the end of the set.
        if (ts.compare(Op.GT, set.end()) ||
          boundary.compare(Op.GT, set.end())) {
          System.out.println("[[[[ exiting as we've hit the end ]]]]");
          break;
        }
        
        if (ts.compare(Op.GTE, boundary)) {
          System.out.println("      flushing or filling.... " + (ts.msEpoch() - boundary.msEpoch()));
          runAccumulatorOrFill(ts.msEpoch());
        }
        
        if ((values[idx] & NumericLongArrayType.FLOAT_FLAG) != 0) {
          if (double_array == null && long_array == null) {
            initDouble();
          }
          
          addLocal(Double.longBitsToDouble(values[idx + 1]));
          idx += 2;
        } else {
          if (double_array == null && long_array == null) {
            initLong();
          }
          
          addLocal(values[idx + 1]);
          idx += 2;
        }
      }
    }
    
    System.out.println("AIDX: " + accumulator_idx + "  idx " + idx + " L: " + values.length + "  B: " + (set.end().epoch() - boundary.epoch()));
    if (next.epoch() > 0) {
      // advance so we can see if we need to run the last bucket or not.
      next.update(series.set().end());
    }
    if (accumulator_idx > 0 && boundary.compare(Op.LTE, set.end()) &&
        // if we're running an odd interval with multis and we've reached the end
        // of an interval  that overlaps a source set boundary then we need to 
        // leave the accumulated data in the array till we get the next pts.
        //idx >= ((NumericLongArrayType) series.value()).end() &&
        (next.epoch() > 0 ? boundary.compare(Op.LTE, next) : true)) {
      System.out.println("         RUNNING ACCUMULATOR");
      runAccumulatorOrFill(boundary.msEpoch());
    }
  }
  
  void initLong() {
    System.out.println("       INIT LONG!!!!");
    value_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
    long_array = (long[]) value_array.object();
    if (long_array.length <= ((DownsamplePartialTimeSeriesSet) set).arraySize()) {
      // ugg the pool is too small.
      // TODO - get a size from the pool BEFORE we claim it.
      value_array.release();
      value_array = null;
      long_array = new long[((DownsamplePartialTimeSeriesSet) set).arraySize()];
    }
    
    accumulator_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
    accumulator_long_array = (long[]) accumulator_array.object();
  }
  
  void initDouble() {
    value_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
    double_array = (double[]) value_array.object();
    if (double_array.length <= ((DownsamplePartialTimeSeriesSet) set).arraySize()) {
      // ugg the pool is too small.
      // TODO - get a size from the pool BEFORE we claim it.
      value_array.release();
      value_array = null;
      double_array = new double[((DownsamplePartialTimeSeriesSet) set).arraySize()];
    }
    accumulator_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
    accumulator_double_array = (double[]) accumulator_array.object();
  }
}


package net.opentsdb.query.processor.downsample;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BasePartialTimeSeries;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
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
  TimeStamp boundary = new SecondTimeStamp(-1);
  TimeStamp next = new SecondTimeStamp(-1);
  
  @Override
  public void reset(final DownsamplePartialTimeSeriesSet set, final boolean multiples) {
    this.set = set; // this is the new downsample set.
    series_list = Lists.newArrayList();
    node = set.node;
  }
  
  /** The current write index for array stores. */
  protected int write_idx;
  
  protected DownsampleNumericPartialTimeSeries(final TSDB tsdb) {
    super();
    this.tsdb = tsdb;
  }
  
  @Override
  public void addSeries(final PartialTimeSeries series) {
    System.out.println(" [[[[[ds]]]] got series");
    id_hash = series.idHash();
    id_type = series.idType();
    if (id_type == null) {
      id_type = series.idType();
    }
    
    if (((DownsamplePartialTimeSeriesSet) set).set_boundaries == null) {
      // WOOT! Simple case where we just agg and send it up
      runSingle(series);
      System.out.println("       SENT UP");
      ((DownsamplePartialTimeSeriesSet) set).node.sendUpstream(this);
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
    this.baseRelease();
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
    long[] values = ((NumericLongArrayType) series.value()).data();
    int idx = ((NumericLongArrayType) series.value()).offset();
    
    boundary.update(set.start());
    boundary.add(((DownsampleConfig) node.config()).interval()); 
    while (idx <= ((NumericLongArrayType) series.value()).end() && idx < 
        ((NumericLongArrayType) series.value()).end()) {
      long ts = 0; // in ms.  TODO - nanos
      if((values[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
        ts = values[idx] & NumericLongArrayType.TIMESTAMP_MASK;
      } else {
        ts = (values[idx] & NumericLongArrayType.TIMESTAMP_MASK) * 1000;
      }
      if (ts >= boundary.msEpoch()) {
        runAccumulatorOrFill(ts);
      }
      
      if ((values[idx] & NumericLongArrayType.FLOAT_FLAG) != 0) {
        if (double_array == null && long_array == null) {
          System.out.println("       INIT DOUBLE!!!!");
          value_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
          double_array = (double[]) value_array.object();
          if (double_array.length <= ((DownsamplePartialTimeSeriesSet) set).size) {
            // ugg the pool is too small.
            // TODO - get a size from the pool BEFORE we claim it.
            value_array.release();
            value_array = null;
            double_array = new double[((DownsamplePartialTimeSeriesSet) set).size];
          }
          
          accumulator_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
          accumulator_double_array = (double[]) accumulator_array.object();
        }
        
        addLocal(Double.longBitsToDouble(values[idx + 1]));
        idx += 2;
      } else {
        if (double_array == null && long_array == null) {
          System.out.println("       INIT LONG!!!!");
          value_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
          long_array = (long[]) value_array.object();
          if (long_array.length <= ((DownsamplePartialTimeSeriesSet) set).size) {
            // ugg the pool is too small.
            // TODO - get a size from the pool BEFORE we claim it.
            value_array.release();
            value_array = null;
            long_array = new long[((DownsamplePartialTimeSeriesSet) set).size];
          }
          
          accumulator_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
          accumulator_long_array = (long[]) accumulator_array.object();
        }
        
        addLocal(values[idx + 1]);
        idx += 2;
      }
    }
    
    if (accumulator_idx > 0) {
      runAccumulatorOrFill(boundary.msEpoch());
    }
    
    // release resources
    if (accumulator_array != null) {
      accumulator_array.release();
      accumulator_array = null;
    }
    accumulator_long_array = null;
    accumulator_double_array = null;
  }
  
  // NOTE the barrier here. We could reduce the time it's locked but we shouldn't
  // block threads very long and it may be a good idea to back things up a bit
  // here anyway.
  synchronized void runMulti(final PartialTimeSeries series) {
    System.out.println("    [[[ds]]] run multiple in PTS!");
    if (next.epoch() < 0) {
      next.update(set.start());
      boundary.update(next);
      boundary.add(((DownsampleConfig) node.config()).interval()); 
    }
    
    PartialTimeSeries local = series;
    while (true) {
      if (local instanceof NoDataPartialTimeSeries) {
        if (local.set().start().compare(Op.EQ, next)) {
          // run it!
          next.update(local.set().end());
          runAccumulatorOrFill(next.epoch());
//          try {
//            local.close();
//          } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//          }
        } else {
          // buffer since we're missing a piece
          series_list.add(local);
          break;
        }
      } else {
        if (local.set().start().compare(Op.EQ, next)) {
          // run it!
          next.update(local.set().end());
          
          long[] values = ((NumericLongArrayType) local.value()).data();
          int idx = ((NumericLongArrayType) local.value()).offset();
          
          if ((values[idx] & NumericLongArrayType.FLOAT_FLAG) != 0) {
            if (double_array == null && long_array == null) {
              System.out.println("       INIT DOUBLE!!!!");
              value_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
              double_array = (double[]) value_array.object();
              if (double_array.length <= ((DownsamplePartialTimeSeriesSet) set).size) {
                // ugg the pool is too small.
                // TODO - get a size from the pool BEFORE we claim it.
                value_array.release();
                value_array = null;
                double_array = new double[((DownsamplePartialTimeSeriesSet) set).size];
              }
              
              accumulator_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
              accumulator_double_array = (double[]) accumulator_array.object();
            }
            
            addLocal(Double.longBitsToDouble(values[idx + 1]));
            idx += 2;
          } else {
            if (double_array == null && long_array == null) {
              System.out.println("       INIT LONG!!!!");
              value_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
              long_array = (long[]) value_array.object();
              if (long_array.length <= ((DownsamplePartialTimeSeriesSet) set).size) {
                // ugg the pool is too small.
                // TODO - get a size from the pool BEFORE we claim it.
                value_array.release();
                value_array = null;
                long_array = new long[((DownsamplePartialTimeSeriesSet) set).size];
              }
              
              accumulator_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
              accumulator_long_array = (long[]) accumulator_array.object();
            }
            
            addLocal(values[idx + 1]);
            idx += 2;
          }
        } else {
          // buffer since we're missing a piece
          series_list.add(local);
          break;
        }
        
        if (series_list.size() == 0) {
          break;
        }
        
        if (next.compare(Op.EQ, set.end())) {
          break;
        }
        
        // see if we have the next set
        local = null;
        for (int i = 0; i < series_list.size(); i++) {
          final PartialTimeSeries pts = series_list.get(i);
          if (pts.set().start().compare(Op.EQ, next)) {
            local = pts;
            break;
          }
        }
        
        if (local == null) {
          // don't have next yet.
          break;
        }
      }
      
    }
    
    // determine if we send it up or not.
    pts_count++;
    System.out.println("   RAN multi. All in? " + ((DownsamplePartialTimeSeriesSet) set).all_sets_accounted_for.get() + "  Last multi: " + ((DownsamplePartialTimeSeriesSet) set).last_multi + "  Cnt: " + pts_count);
    if (((DownsamplePartialTimeSeriesSet) set).all_sets_accounted_for.get() &&
        pts_count == ((DownsamplePartialTimeSeriesSet) set).last_multi) {
      System.out.println("     ---------- DS ------ Send multi upstream!!!");
      ((DownsamplePartialTimeSeriesSet) set).node.sendUpstream(this);
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
    if (((double[]) new_array.object()).length <= long_array.length) {
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
    if (accumulator_idx > 0) {
      // TODO - agg and put
      if (accumulator_long_array != null) {
        node.aggregator.run(accumulator_long_array, 0, accumulator_idx, mdp);
        if (mdp.isInteger()) {
          long_array[write_idx++] = mdp.longValue();
        } else {
          if (double_array == null) {
            flipFlopMainArray();
          }
          double_array[write_idx++] = mdp.toDouble();
        }
      } else {
        node.aggregator.run(accumulator_double_array, 0, accumulator_idx, ((DownsampleConfig) node.config()).getInfectiousNan(), mdp);
        if (long_array != null && double_array == null) {
          flipFlopMainArray();
        }
        double_array[write_idx++] = mdp.doubleValue();
      }
      System.out.println("       [ds] ran: " + boundary.epoch());
      boundary.add(((DownsampleConfig) node.config()).interval()); 
      accumulator_idx = 0;
    }
    
    while (boundary.msEpoch() < ts) {
      // TODO - proper fill
      if (long_array != null && double_array == null) {
        flipFlopMainArray();
      }
      double_array[write_idx++] = Double.NaN;
      boundary.add(((DownsampleConfig) node.config()).interval()); 
    }
  }
}


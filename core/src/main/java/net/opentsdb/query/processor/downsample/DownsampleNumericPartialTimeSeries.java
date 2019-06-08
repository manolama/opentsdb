package net.opentsdb.query.processor.downsample;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BasePartialTimeSeries;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.utils.DateTime;

public class DownsampleNumericPartialTimeSeries extends 
  BasePartialTimeSeries<NumericArrayType> 
    implements DownsamplePartialTimeSeries<NumericArrayType>, NumericArrayType {

  protected final TSDB tsdb;
  
  protected AtomicInteger pts_count;
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
  
  /** The current write index for array stores. */
  protected int write_idx;
  
  protected DownsampleNumericPartialTimeSeries(final TSDB tsdb) {
    super();
    this.tsdb = tsdb;
  }
  
  @Override
  public void addSeries(final PartialTimeSeries series) {
    id_hash = series.idHash();
    id_type = series.idType();
    if (((DownsamplePartialTimeSeriesSet) set).set_boundaries == null) {
      // WOOT! Simple case where we just agg and send it up
      runSingle(series);
      ((DownsamplePartialTimeSeriesSet) set).node.sendUpstream(this);
      return;
    } else if (!(series instanceof NoDataPartialTimeSeries)) {
      // UGG!!! We have to follow the complex and ugly multi-series-path
    }
    
    // determine if we send it up or not.
    final int count = pts_count.incrementAndGet();
    if (((DownsamplePartialTimeSeriesSet) set).all_sets_accounted_for.get() &&
        count == ((DownsamplePartialTimeSeriesSet) set).last_multi) {
      ((DownsamplePartialTimeSeriesSet) set).node.sendUpstream(this);
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
    return value_array.object() instanceof long[];
  }

  @Override
  public long[] longArray() {
    return (long[]) value_array.object();
  }

  @Override
  public double[] doubleArray() {
    return (double[]) value_array.object();
  }

  @Override
  public void reset(final DownsamplePartialTimeSeriesSet set, final boolean multiples) {
    this.set = set; // this is the new downsample set.
    pts_count = new AtomicInteger();
    node = set.node;
  }
  
  void runSingle(final PartialTimeSeries series) {
    long[] values = ((NumericLongArrayType) series.value()).data();
    int idx = ((NumericLongArrayType) series.value()).offset();
    
    final TimeStamp boundary = set.start().getCopy();
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
        
        // flip it
        if (accumulator_double_array == null) {
          final PooledObject temp_object = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
          if (((double[]) temp_object.object()).length <= accumulator_long_array.length) {
            // too long!
            temp_object.release();
            accumulator_double_array = new double[accumulator_long_array.length];
          } else {
            accumulator_double_array = (double[]) temp_object.object();
          }
          
          for (int i = 0; i < accumulator_idx; i++) {
            accumulator_double_array[i] = accumulator_long_array[i];
          }
          accumulator_array.release();
          accumulator_array = temp_object;
          accumulator_long_array = null;
        }
        
        if (accumulator_double_array.length <= accumulator_idx) {
          // too big for pool or local
          accumulator_double_array = Arrays.copyOf(accumulator_double_array, accumulator_double_array.length + 32);
          if (accumulator_array != null) {
            accumulator_array.release();
            accumulator_array = null;
          }
        }
        accumulator_double_array[accumulator_idx++] = Double.longBitsToDouble(values[idx + 1]);
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
        
        if (accumulator_long_array == null) {
          if (accumulator_double_array.length <= accumulator_idx) {
            // too big for pool or local
            accumulator_double_array = Arrays.copyOf(accumulator_double_array, accumulator_double_array.length + 32);
            if (accumulator_array != null) {
              accumulator_array.release();
              accumulator_array = null;
            }
          }
          accumulator_double_array[accumulator_idx++] = values[idx + 1];
        } else {
          if (accumulator_long_array.length <= accumulator_idx) {
            // too big for pool or local
            accumulator_long_array = Arrays.copyOf(accumulator_long_array, accumulator_long_array.length + 32);
            if (accumulator_array != null) {
              accumulator_array.release();
              accumulator_array = null;
            }
          }
          accumulator_long_array[accumulator_idx++] = values[idx + 1];
        }
        idx += 2;
      }
    }
    
    // release resources
    if (accumulator_array != null) {
      accumulator_array.release();
      accumulator_array = null;
    }
    accumulator_long_array = null;
    accumulator_double_array = null;
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
}


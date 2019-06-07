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
  
  /** The current write index for array stores. */
  protected int write_idx;
  
  protected DownsampleNumericPartialTimeSeries(final TSDB tsdb) {
    super();
    this.tsdb = tsdb;
  }
  
  @Override
  public void addSeries(final PartialTimeSeries series) {
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
    
    PooledObject local_array = null;
    long[] local_long_array = null;
    double[] local_double_array = null;
    int local_idx = 0;
    MutableNumericValue mdp = new MutableNumericValue();
    
    final TimeStamp boundary = set.start().getCopy();
    boundary.add(((DownsampleConfig) node.config()).interval()); 
    System.out.println("   BOUNDARY: " + boundary);
    while (idx < ((NumericLongArrayType) series.value()).end()) {
      long ts = 0; // in ms.  TODO - nanos
      if((values[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
        ts = values[idx] & NumericLongArrayType.TIMESTAMP_MASK;
      } else {
        ts = (values[idx] & NumericLongArrayType.TIMESTAMP_MASK) * 1000;
      }
      if (ts >= boundary.msEpoch()) {
        if (local_idx > 0) {
          // TODO - agg and put
          if (local_long_array != null) {
            node.aggregator.run(local_long_array, 0, local_idx, mdp);
            if (double_array != null) {
              double_array[write_idx++] = mdp.toDouble();
            } else {
              long_array[write_idx++] = mdp.longValue();
            }
          } else {
            node.aggregator.run(local_double_array, 0, local_idx, ((DownsampleConfig) node.config()).getInfectiousNan(), mdp);
            if (long_array != null && double_array == null) {
              flipflop();
            }
            double_array[write_idx++] = mdp.doubleValue();
          }
          boundary.add(((DownsampleConfig) node.config()).interval()); 
          local_idx = 0;
        }
        
        while (boundary.msEpoch() < ts) {
          // TODO - proper fill
          if (long_array != null && double_array == null) {
            flipflop();
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
          
          local_array = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
          local_double_array = (double[]) local_array.object();
        }
        
        // flip it
        if (local_double_array == null) {
          final PooledObject temp_object = tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE).claim();
          if (((double[]) temp_object.object()).length <= local_long_array.length) {
            // too long!
            temp_object.release();
            local_double_array = new double[local_long_array.length];
          } else {
            local_double_array = (double[]) temp_object.object();
          }
          
          for (int i = 0; i < local_idx; i++) {
            local_double_array[i] = local_long_array[i];
          }
          local_array.release();
          local_array = temp_object;
          local_long_array = null;
        }
        
        if (local_double_array.length <= local_idx) {
          // too big for pool or local
          local_double_array = Arrays.copyOf(local_double_array, local_double_array.length + 32);
          if (local_array != null) {
            local_array.release();
            local_array = null;
          }
        }
        local_double_array[local_idx++] = Double.longBitsToDouble(values[idx + 1]);
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
          
          local_array = tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE).claim();
          local_long_array = (long[]) local_array.object();
        }
        
        if (local_long_array == null) {
          if (local_double_array.length <= local_idx) {
            // too big for pool or local
            local_double_array = Arrays.copyOf(local_double_array, local_double_array.length + 32);
            if (local_array != null) {
              local_array.release();
              local_array = null;
            }
          }
          local_double_array[local_idx++] = values[idx + 1];
        } else {
          if (local_long_array.length <= local_idx) {
            // too big for pool or local
            local_long_array = Arrays.copyOf(local_long_array, local_long_array.length + 32);
            if (local_array != null) {
              local_array.release();
              local_array = null;
            }
          }
          local_long_array[local_idx++] = values[idx + 1];
        }
        idx += 2;
      }
    }
  }
  
  void flipflop() {
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


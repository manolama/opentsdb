package net.opentsdb.query.processor.downsample;

import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BasePartialTimeSeries;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;
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
      // TODO - agg
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
  }
  
  void runSingle(final PartialTimeSeries series) {
    long[] values = ((NumericLongArrayType) series.value()).data();
    int idx = ((NumericLongArrayType) series.value()).offset();
    
    PooledObject local_array = null;
    long[] local_long_array = null;
    double[] local_double_array = null;
    int local_idx = 0;
    
    while (idx < ((NumericLongArrayType) series.value()).end()) {
      long ts = 0;
      if((values[idx] & NumericLongArrayType.MILLISECOND_FLAG) != 0) {
        ts = (values[idx] & NumericLongArrayType.TIMESTAMP_MASK) / 1000;
      } else {
        ts = values[idx] & NumericLongArrayType.TIMESTAMP_MASK;
      }
      
      if ((values[idx] & NumericLongArrayType.FLOAT_FLAG) != 0) {
        if (double_array == null && long_array == null && value_array == null) {
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
        
        
        
      } else {
        if (double_array == null && long_array == null && value_array == null) {
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
      }
    }
  }
}


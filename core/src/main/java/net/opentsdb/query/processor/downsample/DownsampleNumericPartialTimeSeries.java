package net.opentsdb.query.processor.downsample;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BasePartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.pools.PooledObject;

public class DownsampleNumericPartialTimeSeries extends 
  BasePartialTimeSeries<NumericArrayType> 
    implements DownsamplePartialTimeSeries<NumericArrayType>, NumericArrayType {

  protected final TSDB tsdb;
  
  protected int pts_count;
  protected PartialTimeSeries[] expected;
  protected Downsample node;
  protected int expected_count;
  protected PooledObject value_array;
  
  /** The current write index for array stores. */
  protected int write_idx;
  
  protected DownsampleNumericPartialTimeSeries(final TSDB tsdb) {
    super();
    this.tsdb = tsdb;
    expected = new PartialTimeSeries[1];
  }
  
  @Override
  public void addSeries(final PartialTimeSeries series) {
    
  }
  
  @Override
  public TimeSeriesDataType value() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void release() {
    if (value_array != null) {
      value_array.release();
    }
    
    // TODO reset of it
    this.baseRelease();
  }
  
  @Override
  public int offset() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int end() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isInteger() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long[] longArray() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double[] doubleArray() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void reset(final Downsample node, final PartialTimeSeriesSet set, final boolean multiples) {
    this.set = set; // this is the new downsample set.
    pts_count = 0;
    expected_count = 1; // TODO - compute and set
    if (expected_count >= expected.length) {
      expected = new PartialTimeSeries[expected_count];
    }
  }
  
  protected void add(final PartialTimeSeries pts) {
    id_hash = pts.idHash();
    expected[0] = pts;
    if (++pts_count == expected_count) {
      // run it
      
    }
  }
}


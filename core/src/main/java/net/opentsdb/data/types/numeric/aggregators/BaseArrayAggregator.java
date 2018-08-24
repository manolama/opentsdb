package net.opentsdb.data.types.numeric.aggregators;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericArrayType;

public abstract class BaseArrayAggregator implements NumericArrayAggregator {

  protected final boolean infectious_nans;
  
  protected long[] long_accumulator;
  
  protected double[] double_accumulator;

  public BaseArrayAggregator(final boolean infectious_nans) {
    this.infectious_nans = infectious_nans;
  }
  
  @Override
  public void accumulate(final long[] values) {
    accumulate(values, 0, values.length);
  }
  
  @Override
  public void accumulate(final double[] values) {
    accumulate(values, 0, values.length);
  }
  
  @Override
  public boolean isInteger() {
    return long_accumulator == null ? false : true;
  }

  @Override
  public long[] longArray() {
    return long_accumulator;
  }

  @Override
  public double[] doubleArray() {
    return double_accumulator;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericArrayType.TYPE;
  }

}

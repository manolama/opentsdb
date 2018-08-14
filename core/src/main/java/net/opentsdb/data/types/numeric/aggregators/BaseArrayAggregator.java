package net.opentsdb.data.types.numeric.aggregators;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.NumericArrayType;

public abstract class BaseArrayAggregator implements NumericArrayAggregator {

  protected long[] long_accumulator;
  
  protected double[] double_accumulator;

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

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
}

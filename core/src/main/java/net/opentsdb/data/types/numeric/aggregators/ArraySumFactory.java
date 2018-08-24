package net.opentsdb.data.types.numeric.aggregators;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

public class ArraySumFactory implements NumericArrayAggregatorFactory {

  @Override
  public NumericArrayAggregator newAggregator(boolean infectious_nan) {
    return new ArraySum(infectious_nan);
  }
  
  @Override
  public String id() {
    return "sum";
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

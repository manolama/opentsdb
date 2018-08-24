package net.opentsdb.data.types.numeric.aggregators;

import net.opentsdb.core.TSDBPlugin;

public interface NumericArrayAggregatorFactory extends TSDBPlugin {

  public NumericArrayAggregator newAggregator(final boolean infectious_nan);
  
}

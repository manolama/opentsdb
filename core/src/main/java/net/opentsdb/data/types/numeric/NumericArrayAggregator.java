package net.opentsdb.data.types.numeric;

import net.opentsdb.core.TSDBPlugin;

public interface NumericArrayAggregator extends NumericArrayType, TSDBPlugin {

  public void accumulate(final long[] values);
  
  public void accumulate(final double[] values,
                         final boolean infectious_nans);
  
}

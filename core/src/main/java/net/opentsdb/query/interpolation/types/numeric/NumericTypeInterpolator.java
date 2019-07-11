package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;

/**
 * For now we're just dealing with stateless one value on either side interpolators.
 */
public interface NumericTypeInterpolator {

  public void fill(
      final NumericInterpolatorConfig config,
      final TimeStamp target,
      final MutableNumericValue left,
      final MutableNumericValue right,
      final MutableNumericType value);
  
}

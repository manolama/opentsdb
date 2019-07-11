package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.core.TSDBPlugin;

public interface NumericInterpolatorFactory extends TSDBPlugin {

  /**
   * Returns a new interpolator for the given data type if present. This is for 
   * newer style interpolators that perform similar to aggregators and can be
   * statically instantiated.
   * @return An instantiated interpolator or null if an implementation does not
   * exist for the requested type. In such an event, the series should log and 
   * be dropped.
   */
  public NumericTypeInterpolator newInterpolator();
  
}

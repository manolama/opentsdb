package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;

public class FillingNumericInterpolatorFactory extends BaseTSDBPlugin 
    implements NumericInterpolatorFactory {

  private final static NumericTypeInterpolator INTERPOLATOR = 
      new FillingNumericInterpolator();
  
  private static class FillingNumericInterpolator implements 
      NumericTypeInterpolator {

    @Override
    public void fill(final NumericInterpolatorConfig config,
                     final TimeStamp target,
                     final MutableNumericValue left,
                     final MutableNumericValue right,
                     final MutableNumericType value) {
      if ((left == null || left.value() == null) && 
          (right == null || right.value() == null)) {
        value.set(config.queryFill().fill());
      } else if ((right != null && right.value() != null) && 
          (config.getRealFillPolicy() == FillWithRealPolicy.NEXT_ONLY ||
          config.getRealFillPolicy() == FillWithRealPolicy.PREFER_NEXT)) {
        value.set(right);
      } else if ((left != null && left.value() != null) && 
            (config.getRealFillPolicy() == FillWithRealPolicy.PREVIOUS_ONLY ||
            config.getRealFillPolicy() == FillWithRealPolicy.PREFER_PREVIOUS)) {
        value.set(left);
      } else {
        value.set(config.queryFill().fill());
      }
    }

  }

  @Override
  public NumericTypeInterpolator newInterpolator() {
    return INTERPOLATOR;
  }

  @Override
  public String type() {
    return null;
  }

  @Override
  public String version() {
    return "3.0.0";
  }
  
}

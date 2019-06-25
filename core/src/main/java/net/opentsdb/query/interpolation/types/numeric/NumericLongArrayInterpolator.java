package net.opentsdb.query.interpolation.types.numeric;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

public interface NumericLongArrayInterpolator {

  public void fill(
      final QueryInterpolatorConfig config,
      final TimeStamp target,
      final NumericLongArrayType left,
      final int left_offset,
      final NumericLongArrayType right,
      final int right_offset,
      final MutableNumericType value);
  
  public void fill(
      final TimeStamp target,
      final NumericLongArrayType left,
      final int left_offset,
      final NumericLongArrayType right,
      final int right_offset,
      final MutableNumericType value);
  
}

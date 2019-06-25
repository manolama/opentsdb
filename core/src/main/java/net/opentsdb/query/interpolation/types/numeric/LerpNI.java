package net.opentsdb.query.interpolation.types.numeric;

import java.time.temporal.ChronoUnit;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

public class LerpNI implements NumericLongArrayInterpolator {
  
  @Override
  public void fill(final QueryInterpolatorConfig config,
      TimeStamp target, NumericLongArrayType left,
      int left_offset, NumericLongArrayType right, int right_offset,
      MutableNumericType value) {
    
    final NumericInterpolatorConfig cfg = (NumericInterpolatorConfig) config;
    
    if (left == null || left_offset >= left.end()) {
      // fill
      if (right.data() != null && 
          cfg.getRealFillPolicy() == FillWithRealPolicy.NEXT_ONLY ||
          cfg.getRealFillPolicy() == FillWithRealPolicy.PREFER_NEXT) {
        if (NumericLongArrayType.isDouble(right.data(), right_offset)) {
          value.set(NumericLongArrayType.getDouble(right.data(), right_offset));
        } else {
          value.set(NumericLongArrayType.getLong(right.data(), right_offset));
        }
      } else {
        value.set(cfg.queryFill().fill());
      }
    } else if (right == null || right_offset >= left.end()) {
      // fill
      if (left.data() != null && 
          cfg.getRealFillPolicy() == FillWithRealPolicy.NEXT_ONLY ||
          cfg.getRealFillPolicy() == FillWithRealPolicy.PREFER_NEXT) {
        if (NumericLongArrayType.isDouble(left.data(), left_offset)) {
          value.set(NumericLongArrayType.getDouble(left.data(), left_offset));
        } else {
          value.set(NumericLongArrayType.getLong(left.data(), left_offset));
        }
      } else {
        value.set(cfg.queryFill().fill());
      }
    } else {
      // lerp it!
      long left_epoch;
      long left_nano;
      long epoch_delta = 0;
      long nano_delta = 0;
      final ChronoUnit right_units = NumericLongArrayType.timestampUnits(right.data(), right_offset);
      switch (right_units) {
      case NANOS:
      case SECONDS:
        epoch_delta = right.data()[right_offset] & NumericLongArrayType.TIMESTAMP_MASK;
        nano_delta = right.data()[right_offset + 1];
        break;
      case MILLIS:
        epoch_delta = (right.data()[right_offset] & NumericLongArrayType.TIMESTAMP_MASK) / 1000;
        nano_delta = (((right.data()[right_offset] & NumericLongArrayType.TIMESTAMP_MASK)) - epoch_delta) * 1000000;
        break;
      default:
        throw new IllegalStateException("Invalid right units: " + right_units);
      }
      
      final ChronoUnit left_units = NumericLongArrayType.timestampUnits(left.data(), left_offset);
      switch (left_units) {
      case NANOS:
      case SECONDS:
        left_epoch = left.data()[left_offset] & NumericLongArrayType.TIMESTAMP_MASK;
        epoch_delta -= left_epoch;
        left_nano = left.data()[left_offset + 1];
        nano_delta -= left_nano;
        break;
      case MILLIS:
        left_epoch = (left.data()[left_offset] & NumericLongArrayType.TIMESTAMP_MASK) / 1000;
        left_nano = ((left.data()[left_offset] & NumericLongArrayType.TIMESTAMP_MASK) - left_epoch) * 1000000;
        nano_delta -= left_nano;
        epoch_delta -= left_epoch;
        break;
      default:
        throw new IllegalStateException("Invalid left units: " + left_units);
      }
      
      final long lerp_epoch_delta = target.epoch() - left_epoch;
      final long lerp_nano_delta = target.nanos() - left_nano;
      if (NumericLongArrayType.isDouble(left.data(), left.offset()) || 
          NumericLongArrayType.isDouble(right.data(), right_offset)) {
        double value_delta = NumericLongArrayType.isDouble(right.data(), right_offset) ?
            NumericLongArrayType.getDouble(right.data(), right_offset, right_units) :
              NumericLongArrayType.getLong(right.data(), right_offset, right_units);
        value_delta -= NumericLongArrayType.isDouble(left.data(), left_offset) ?
            NumericLongArrayType.getDouble(left.data(), left_offset, left_units) :
              NumericLongArrayType.getLong(left.data(), left_offset, left_units);
            final double lerp = value_delta / (double) (
                (lerp_epoch_delta * 1000000000L) + lerp_nano_delta);
            // TODO see if it really is a long
            value.set(lerp);
      } else {
        // both longs, yay!
        final long value_delta = NumericLongArrayType.getLong(
            right.data(), right_offset, right_units) -
            NumericLongArrayType.getLong(left.data(), left_offset, left_units);
        // TODO - beware massive deltas!
        final double lerp = ((double) value_delta) / (double) (
            (lerp_epoch_delta * 1000000000L) + lerp_nano_delta);
        // TODO see if it really is a long
        value.set(lerp);
      }
    }
    
  }

}

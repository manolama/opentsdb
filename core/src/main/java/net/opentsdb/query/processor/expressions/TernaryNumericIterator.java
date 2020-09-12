package net.opentsdb.query.processor.expressions;

import java.util.Map;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;

public class TernaryNumericIterator extends ExpressionNumericIterator {
  
  protected TypedTimeSeriesIterator condition;
  
  TernaryNumericIterator(final QueryNode node, 
                         final QueryResult result,
                         final Map<String, TimeSeries> sources) {
    super(node, result, sources);
    
    TimeSeries c = sources.get(ExpressionTimeSeries.CONDITION_KEY);
    if (c == null) {
      // can't do anything
      has_next = false;
      return;
    }
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
        c.iterator(NumericType.TYPE);
    if (!op.isPresent()) {
      // can't do anything so leave has_next as false.
      has_next = false;
      return;
    }
    condition = op.get();
    has_next = condition.hasNext();
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    TimeSeriesValue<NumericType> c = (TimeSeriesValue<NumericType>) condition.next();
    has_next = condition.hasNext();
    boolean is_true = false;
    if (c.value() != null) {
      if (c.value().isInteger()) {
        if (c.value().longValue() > 0) {
          is_true = true;
        } 
      } else if (Double.isFinite(c.value().doubleValue()) && 
                 c.value().doubleValue() > 0) {
        is_true = true;
      }
    }
    System.out.println("                NEXT");
    if (is_true) {
      if (left_interpolator == null) {
        if (left_literal == null) {
          // TODO - real substitute!
          dp.reset(c.timestamp(), ZERO_SUBSTITUTE);
        } else {
          dp.reset(c.timestamp(), left_literal);
        }
      } else {
        dp.reset(left_interpolator.next(c.timestamp()));
      }
    } else {
      if (right_interpolator == null) {
        if (right_literal == null) {
          // TODO - real substitute!
          dp.reset(c.timestamp(), ZERO_SUBSTITUTE);
        } else {
          dp.reset(c.timestamp(), right_literal);
        }
      } else {
        dp.reset(right_interpolator.next(c.timestamp()));
      }
    }
    return dp;
  }
}

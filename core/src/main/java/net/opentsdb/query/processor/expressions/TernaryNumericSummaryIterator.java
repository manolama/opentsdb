package net.opentsdb.query.processor.expressions;

import java.util.Map;
import java.util.Optional;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

public class TernaryNumericSummaryIterator extends ExpressionNumericSummaryIterator {

  protected TypedTimeSeriesIterator condition;
  
  TernaryNumericSummaryIterator(final QueryNode node, 
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
        c.iterator(NumericSummaryType.TYPE);
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
    TimeSeriesValue<NumericSummaryType> c = (TimeSeriesValue<NumericSummaryType>) condition.next();
    has_next = condition.hasNext();
    
    if (c.value() != null) {
      TimeSeriesValue<NumericSummaryType> l = left_interpolator.next(c.timestamp());
      TimeSeriesValue<NumericSummaryType> r = right_interpolator.next(c.timestamp());
      for (int summary : c.value().summariesAvailable()) {
        NumericType v = c.value().value(summary);
        boolean is_true = false;
        if (v.isInteger()) {
          if (v.longValue() > 0) {
            is_true = true;
          } 
        } else if (Double.isFinite(v.doubleValue()) && 
                   v.doubleValue() > 0) {
          is_true = true;
        }
        
        if (is_true) {
          if (l == null) {
            dp.nullSummary(summary);
          } else {
            dp.resetValue(summary, l.value().value(summary));
          }
        } else {
          if (r == null) {
            dp.nullSummary(summary);
          } else {
            dp.resetValue(summary, r.value().value(summary));
          }
        }
      }
    } else {
      dp.resetNull(c.timestamp());
    }
    
    dp.resetTimestamp(c.timestamp());
    return dp;
  }
}

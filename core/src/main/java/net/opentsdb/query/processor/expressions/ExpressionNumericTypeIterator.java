package net.opentsdb.query.processor.expressions;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.BranchType;

public class ExpressionNumericTypeIterator implements QueryIterator, 
    TimeSeriesValue<NumericType> {

  static final double EPSILON = Math.ulp(1.0);
  
  final ExpressionTimeSeries series;
  
  /** The next timestamp to return. */
  private final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaluated when returning the next value. */
  private final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericValue dp;
  
  QueryInterpolator<NumericType> left_interpolator;
  QueryInterpolator<NumericType> right_interpolator;
  
  NumericType left_literal;
  NumericType right_literal;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  ExpressionNumericTypeIterator(final ExpressionTimeSeries series) {
    this.series = series;
    dp = new MutableNumericValue();
    next_ts.setMax();
    
    if (series.left == null) {
      left_interpolator = null;
      left_literal = buildLiteral(series.node.exp_config.left, series.node.exp_config.left_type);
    } else {
      // TODO - get the specific config if present
      QueryInterpolatorConfig interpolator_config = series.node.config
          .interpolatorConfig(NumericType.TYPE);
      QueryInterpolatorFactory factory = series.node.pipelineContext().tsdb()
          .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
              interpolator_config.id());
      if (factory == null) {
        throw new IllegalArgumentException("No interpolator factory found for: " + 
            (interpolator_config.id() == null ? "Default" : interpolator_config.id()));
      }
      
      System.out.println("   LEFT SERIES: " + series.left);
      left_interpolator = (QueryInterpolator<NumericType>) factory.newInterpolator(NumericType.TYPE, series.left, interpolator_config);
      left_literal = null;
      has_next = left_interpolator.hasNext();
      if (has_next) {
        next_ts.update(left_interpolator.nextReal());
      }
    }
    
    if (series.right == null) {
      right_interpolator = null;
      right_literal = buildLiteral(series.node.exp_config.right, series.node.exp_config.right_type);
      System.out.println("RIGHT is null........");
    } else {
      // TODO - get the specific config if present
      QueryInterpolatorConfig interpolator_config = series.node.config.interpolatorConfig(NumericType.TYPE);
      QueryInterpolatorFactory factory = series.node.pipelineContext().tsdb()
          .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
              interpolator_config.id());
      if (factory == null) {
        throw new IllegalArgumentException("No interpolator factory found for: " + 
            interpolator_config.id() == null ? "Default" : interpolator_config.id());
      }
      
      System.out.println("   RIGHT SERIES: " + series.right);
      right_interpolator = (QueryInterpolator<NumericType>) factory.newInterpolator(NumericType.TYPE, series.right, interpolator_config);
      right_literal = null;
      if (!has_next) {
        has_next = right_interpolator.hasNext();
        next_ts.update(right_interpolator.nextReal());
      } else {
        if (right_interpolator.nextReal().compare(Op.LT, next_ts)) {
          next_ts.update(right_interpolator.nextReal());
        }
      }
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    next_next_ts.setMax();
    
    if (left_interpolator != null && right_interpolator != null) {
      TimeSeriesValue<NumericType> l = left_interpolator.next(next_ts);
      TimeSeriesValue<NumericType> r = right_interpolator.next(next_ts);
      
//      System.out.println("   L: " + l);
//      System.out.println("   R: " + r);
      // TODO - nulls??
      setValue(l.value(), r.value());
      
      if (left_interpolator.hasNext()) {
        has_next = true;
        next_next_ts.update(left_interpolator.nextReal());
      }
      if (right_interpolator.hasNext()) {
        has_next = true;
        if (right_interpolator.nextReal().compare(Op.LT, next_next_ts)) {
          next_next_ts.update(right_interpolator.nextReal());
        }
      }
    } else if (left_interpolator == null) {
      TimeSeriesValue<NumericType> r = right_interpolator.next(next_ts);
      // TODO - nulls??
      setValue(left_literal, r.value());
      if (right_interpolator.hasNext()) {
        has_next = true;
        next_next_ts.update(right_interpolator.nextReal());
      }
    } else {
      TimeSeriesValue<NumericType> l = left_interpolator.next(next_ts);
      // TODO - nulls??
      setValue(l.value(), right_literal);
      System.out.println("RIGHT LITERAL: " + right_literal);
      if (left_interpolator.hasNext()) {
        has_next = true;
        next_next_ts.update(left_interpolator.nextReal());
      }
    }
    
    next_ts.update(next_next_ts);
    return dp;
  }

  @Override
  public TimeStamp timestamp() {
    return dp.timestamp();
  }

  @Override
  public NumericType value() {
    return dp;
  }

  @Override
  public TypeToken<NumericType> type() {
    return NumericType.TYPE;
  }

  // TODO - nulls?
  void setValue(final NumericType left, final NumericType right) {
    dp.resetTimestamp(next_ts);
    switch (series.node.exp_config.op) {
    case OR:
      if (left == null) {
        dp.resetValue(isTrue(right) ? 1 : 0);
      } else if (right == null) {
        dp.resetValue(isTrue(left) ? 1 : 0);
      } else {
        dp.resetValue(isTrue(left) || isTrue(right) ? 1 : 0);
      }
      break;
    case AND:
      if (left == null || right == null) {
        dp.resetValue(0);
      } else {
        dp.resetValue(isTrue(left) && isTrue(right) ? 1 : 0);
      }
      break;
    case EQ:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() == right.longValue() ? 1 : 0);
      } else {
        dp.resetValue(Math.abs(left.doubleValue() - right.doubleValue()) <= EPSILON ? 1 : 0);
      }
      break;
    case NE:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() != right.longValue() ? 1 : 0);
      } else {
        dp.resetValue(Math.abs(left.doubleValue() - right.doubleValue()) > EPSILON ? 1 : 0);
      }
      break;
    case LT:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() < right.longValue() ? 1 : 0);
      } else {
        if (Math.abs(left.doubleValue() - right.doubleValue()) <= EPSILON) {
          dp.resetValue(0);
        } else {
          dp.resetValue(left.doubleValue() < right.doubleValue() ? 1 : 0);
        }
      }
      break;
    case GT:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() > right.longValue() ? 1 : 0);
      } else {
        if (Math.abs(left.doubleValue() - right.doubleValue()) <= EPSILON) {
          dp.resetValue(0);
        } else {
          dp.resetValue(left.doubleValue() > right.doubleValue() ? 1 : 0);
        }
      }
      break;
    case LE:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() < right.longValue() ? 1 : 0);
      } else {
        if (Math.abs(left.doubleValue() - right.doubleValue()) <= EPSILON) {
          dp.resetValue(1);
        } else {
          dp.resetValue(left.doubleValue() < right.doubleValue() ? 1 : 0);
        }
      }
      break;
    case GE:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() > right.longValue() ? 1 : 0);
      } else {
        if (Math.abs(left.doubleValue() - right.doubleValue()) <= EPSILON) {
          dp.resetValue(1);
        } else {
          dp.resetValue(left.doubleValue() > right.doubleValue() ? 1 : 0);
        }
      }
      break;
    case ADD:
      if (left.isInteger() && right.isInteger()) {
        // TOOD - overflow
        dp.resetValue(left.longValue() + right.longValue());
      } else {
        dp.resetValue(left.toDouble() + right.toDouble());
      }
      break;
    case SUBTRACT:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() - right.longValue());
      } else {
        dp.resetValue(left.toDouble() - right.toDouble());
      }
      break;
    case MULTIPLY:
      if (left.isInteger() && right.isInteger()) {
        // TOOD - overflow
        dp.resetValue(left.longValue() * right.longValue());
      } else {
        dp.resetValue(left.toDouble() * right.toDouble());
      }
      break;
    case DIVIDE:
      if (left.isInteger() && right.isInteger() && 
          left.longValue() % right.longValue() == 0) {
        dp.resetValue(left.longValue() / right.longValue());
      } else {
        dp.resetValue(left.toDouble() / right.toDouble());
      }
      break;
    case MOD:
      if (left.isInteger() && right.isInteger()) {
        dp.resetValue(left.longValue() % right.longValue());
      } else {
        dp.resetValue(left.doubleValue() % right.doubleValue());
      }
      break;
    }
  }
  
  boolean isTrue(NumericType v) {
    if (v.isInteger()) {
      return v.longValue() > 0;
    }
    return v.doubleValue() > 0;
  }

  NumericType buildLiteral(final String literal, BranchType type) {
    switch (type) {
    case NULL:
      return null;
    case LITERAL_BOOL:
      return new MutableNumericType(Boolean.parseBoolean(literal) ? 1L : 0L);
    case LITERAL_NUMERIC:
      if (literal.contains(".")) {
        return new MutableNumericType(Double.parseDouble(literal));
      }
      return new MutableNumericType(Long.parseLong(literal));
    default:
      throw new RuntimeException("INvalid type: " + type);
    }
  }
}

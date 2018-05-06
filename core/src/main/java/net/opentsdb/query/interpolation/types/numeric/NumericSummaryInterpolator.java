package net.opentsdb.query.interpolation.types.numeric;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.pojo.FillPolicy;

public class NumericSummaryInterpolator implements 
    QueryIteratorInterpolator<NumericSummaryType>{

  protected final NumericSummaryInterpolatorConfig config;
  
  protected final QueryFillPolicy<NumericSummaryType> fill_policy;
  
  /** The iterator pulled from the source. May be null. */
  protected final Iterator<TimeSeriesValue<?>> iterator;
  
  /** The previous real value. */
  protected Map<Integer, ReadAheadNumericInterpolator> data;
  
  protected TimeSeriesValue<NumericSummaryType> next;
  
  /** The value filled when lerping. */
  protected final MutableNumericSummaryValue response;
  
  /** Whether or not the source iterator has more data. */
  protected boolean has_next;
  
  public NumericSummaryInterpolator(final TimeSeries source, 
                                    final NumericSummaryInterpolatorConfig config) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = config;
    fill_policy = config.queryFill();
    final Optional<Iterator<TimeSeriesValue<?>>> optional = 
        source.iterator(NumericSummaryType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
    } else {
      iterator = null;
    }
    response = new MutableNumericSummaryValue();
    initialize();
  }
  
  public NumericSummaryInterpolator(final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator, 
                                    final NumericSummaryInterpolatorConfig config) {
    if (iterator == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null.");
    }
    this.config = config;
    fill_policy = config.queryFill();
    this.iterator = iterator;
    response = new MutableNumericSummaryValue();
    initialize();
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @SuppressWarnings("unchecked")
  @Override
  public TimeSeriesValue<NumericSummaryType> next(final TimeStamp timestamp) {
    if (iterator == null || next == null) {
      return fill(timestamp);
    }
    
    has_next = false;
    if (next != null && next.timestamp().compare(RelationalOperator.LTE, timestamp)) {
      if (iterator.hasNext()) {
        if (config.sync()) {
          advanceSynchronized();
        } else {
          next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          setReadAheads(next);
        }
      } else {
        next = null;
      }
    }

    has_next = next != null;
    return fill(timestamp);
  }

  @Override
  public TimeStamp nextReal() {
    if (next == null || !has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  @Override
  public QueryFillPolicy<NumericSummaryType> fillPolicy() {
    return fill_policy;
  }

  @VisibleForTesting
  MutableNumericSummaryValue fill(final TimeStamp timestamp) {
    response.clear();
    response.resetTimestamp(timestamp);
    for (final Entry<Integer, ReadAheadNumericInterpolator> entry : data.entrySet()) {
      response.resetValue(entry.getKey(), entry.getValue().next(timestamp));
    }
    return response;
  }

  @VisibleForTesting
  void setReadAheads(final TimeSeriesValue<NumericSummaryType> value) {
    if (value.value() == null) {
      return;
    }
    
    final MutableNumericValue dp = new MutableNumericValue();
    dp.resetTimestamp(value.timestamp());
    for (final int summary : value.value().summariesAvailable()) {
      ReadAheadNumericInterpolator interpolator = data.get(summary);
      if (interpolator == null) {
        // we don't care about this one.
        // TODO - a counter of these drops
        continue;
      }
      final NumericType v = value.value().value(summary);
      if (v == null) {
        dp.resetNull(value.timestamp());
      } else if (v.isInteger()) {
        dp.resetValue(v.longValue());
      } else {
        dp.resetValue(v.doubleValue());
      }
      interpolator.setNext(dp);
    }
  }

  @SuppressWarnings("unchecked")
  private void advanceSynchronized() {
    // advance to the next real that has values for all summaries
    while (iterator.hasNext()) {
      next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (next.value() == null) {
        continue;
      }
      
      int present = 0;
      for (final int summary : next.value().summariesAvailable()) {
        final NumericType value = next.value().value(summary);
        if (value == null) {
          continue;
        }
        if (!value.isInteger() && Double.isNaN(value.doubleValue())) {
          continue;
        }
        present++;
      }
      
      if (present == config.expectedSummaries().size()) {
        // all there!
        setReadAheads(next);
        has_next = true;
        return;
      }
    }
    
    // we hit the end, nothing left that's in sync.
    next = null;
    has_next = false;
  }
  
  @SuppressWarnings("unchecked")
  private void initialize() {
    data = Maps.newHashMapWithExpectedSize(config.expectedSummaries().size());
    for (final int summary : config.expectedSummaries()) {
      data.put(summary, new ReadAheadNumericInterpolator(config.queryFill(summary)));
    }
    if (iterator != null) {
      if (!config.sync()) {
        if (iterator.hasNext()) {
          next = (TimeSeriesValue<NumericSummaryType>) iterator.next();
          setReadAheads(next);
          has_next = true;
        }
      } else {
        advanceSynchronized();
      }
    }
  }
}

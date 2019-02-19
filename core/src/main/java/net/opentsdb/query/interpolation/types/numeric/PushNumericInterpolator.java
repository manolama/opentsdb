package net.opentsdb.query.interpolation.types.numeric;

import java.util.Iterator;
import java.util.NoSuchElementException;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.interpolation.QueryInterpolator2;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

public class PushNumericInterpolator implements QueryInterpolator2<NumericType> {

  private static final TimeStamp ZERO = new MillisecondTimeStamp(0L);
  
  /** The config. */
  protected NumericInterpolatorConfig config;
  
  /** The previous real value. */
  protected MutableNumericValue previous;
  
  /** The next real value. */
  protected TimeSeriesValue<NumericType> next;
  
  /** The value filled when lerping. */
  protected MutableNumericValue response;
  
  /** Whether or not the source iterator has more data. */
  protected boolean has_next;
  
  // TODO - handle out of order and pre/post series data for proper interp.
  protected PartialTimeSeries current_series;
  
  protected Iterator<TimeSeriesValue<?>> iterator;
  
  public PushNumericInterpolator() {
    response = new MutableNumericValue();
    previous = new MutableNumericValue();
  }
  
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setConfig(QueryInterpolatorConfig config) {
    this.config = (NumericInterpolatorConfig) config;
    previous.resetTimestamp(ZERO);
    // TODO - reset others
  }

  @Override
  public void setSeries(PartialTimeSeries series) {
    // TODO Auto-generated method stub
    current_series = series;
    iterator = current_series.iterator();
    has_next = iterator.hasNext();
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericType> next(TimeStamp timestamp) {
    // if the iterator is null or it was empty, the next is null and we just 
    // pass the fill value.
    if (iterator == null || next == null) {
      return fill(timestamp);
    }
    
    has_next = false;
    if (timestamp.compare(Op.EQ, next.timestamp())) {
      response.reset(next);
      if (previous.timestamp().msEpoch() == 0) {
        previous = new MutableNumericValue(next);
      } else {
        previous.reset(next);
      }
      
      if (iterator.hasNext()) {
        next = (TimeSeriesValue<NumericType>) iterator.next();
        has_next = true;
      } else {
        next = null;
      }
    } else {
      if (next != null) {
        has_next = true;
      }
      return fill(timestamp);
    }
    
    return response;
  }

  @Override
  public TimeStamp nextReal() {
    if (!has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  @Override
  public QueryFillPolicy<NumericType> fillPolicy() {
    return config.queryFill();
  }

  protected TimeSeriesValue<NumericType> fill(final TimeStamp timestamp) {
    switch (config.getRealFillPolicy()) {
    case PREVIOUS_ONLY:
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      break;
    case PREFER_PREVIOUS:
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      break;
    case NEXT_ONLY:
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      break;
    case PREFER_NEXT:
      if (next != null) {
        response.reset(timestamp, next.value());
        return response;
      }
      if (previous != null) {
        response.reset(timestamp, previous.value());
        return response;
      }
      break;
    case NONE:
      break;
    }
    
    final NumericType fill = config.queryFill().fill();
    if (fill == null) {
      response.resetNull(timestamp);
    } else {
      response.reset(timestamp, fill);
    }
    return response;
  }
}

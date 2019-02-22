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
import net.opentsdb.query.interpolation.PartialQueryInterpolator;
import net.opentsdb.query.interpolation.PartialQueryInterpolatorContainer;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

/**
 * 
 * NOTE!!!!!!!!!!!! Big race condition here on {@link #setSeries(PartialTimeSeries)}
 * 
 * Gotta figure this sucker out so it supports interpolating across boundaries
 * and is thread safe/concurrent AND poolable.
 * 
 * could keep PTs ID and a first + last values
 *
 */
public class PartialNumericInterpolator implements PartialQueryInterpolator<NumericType> {

  private static final TimeStamp ZERO = new MillisecondTimeStamp(0L);
  
  /** The previous real value. */
  protected MutableNumericValue previous;
  
  /** The next real value. */
  protected TimeSeriesValue<NumericType> next;
  
  /** The value filled when lerping. */
  protected MutableNumericValue response;
  
  /** Whether or not the source iterator has more data. */
  protected boolean has_next;
  PartialQueryInterpolatorContainer<NumericType> container;
  protected Iterator<TimeSeriesValue<?>> iterator;
  
  public PartialNumericInterpolator() {
    response = new MutableNumericValue();
    previous = new MutableNumericValue();
  }
  
  @Override
  public void reset(PartialQueryInterpolatorContainer<NumericType> container,
      PartialTimeSeries pts) {
    this.container = container;
    iterator = pts.iterator();
  }
  
  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
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
  
  protected TimeSeriesValue<NumericType> fill(final TimeStamp timestamp) {
    switch (((NumericInterpolatorConfig) container.config()).getRealFillPolicy()) {
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
    
    final NumericType fill = ((NumericInterpolatorConfig) container.config()).queryFill().fill();
    if (fill == null) {
      response.resetNull(timestamp);
    } else {
      response.reset(timestamp, fill);
    }
    return response;
  }

  
}

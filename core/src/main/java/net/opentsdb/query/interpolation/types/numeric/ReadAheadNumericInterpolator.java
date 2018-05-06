package net.opentsdb.query.interpolation.types.numeric;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryIteratorInterpolator;

/**
 * for summaries that may need to be synced
 *
 */
public class ReadAheadNumericInterpolator implements 
    QueryIteratorInterpolator<NumericType> {

  protected final QueryFillPolicy<NumericType> fill_policy;
  protected LinkedList<MutableNumericValue> previous;
  protected MutableNumericValue next;
  protected MutableNumericValue response;
  protected boolean has_next;
  
  public ReadAheadNumericInterpolator(
      final QueryFillPolicy<NumericType> fill_policy) {
    if (fill_policy == null) {
      throw new IllegalArgumentException("Fill policy cannot be null.");
    }
    this.fill_policy = fill_policy;
    previous = Lists.newLinkedList();
    response = new MutableNumericValue();
  }
  
  @Override
  public TimeSeriesValue<NumericType> next(final TimeStamp timestamp) {
    int idx = 0;
    for (final MutableNumericValue prev : previous ) {
      if (prev.timestamp().compare(RelationalOperator.EQ, timestamp)) {
        response.reset(prev);
        if (idx > 0) {
          // cleanup previous entries we no longer need.
          for (int i = 0; i < idx; i++) {
            previous.removeFirst();
          }
        }
        return response;
      }
      idx++;
    }
    
    has_next = false;
    if (next != null && timestamp.compare(RelationalOperator.EQ, next.timestamp())) {
      response.reset(next);
      previous.clear();
      previous.add(next);
      next = null;
      return response;
    }
    has_next = next != null;
    
    // fill
    return fill(timestamp);
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeStamp nextReal() {
    if (!has_next) {
      throw new NoSuchElementException();
    }
    return next.timestamp();
  }

  /**
   * TODO - doc
   * <b>NOTE:</b> This method does not check timestamps for ordering.
   * @param next A value or null. When null, if a next value was present, 
   * it is moved to the previous list.
   */
  public void setNext(final TimeSeriesValue<NumericType> next) {
    if (this.next == null) {
      if (next == null) {
        // no-op
        return;
      }
      this.next = new MutableNumericValue(next);
      has_next = true;
      return;
    }
    
    previous.add(this.next);
    if (next == null) {
      this.next = null;
      has_next = false;
    } else {
      this.next = new MutableNumericValue(next);
      has_next = true;
    }
  }
  
  @Override
  public QueryFillPolicy<NumericType> fillPolicy() {
    return fill_policy;
  }

  @VisibleForTesting
  protected TimeSeriesValue<NumericType> fill(final TimeStamp timestamp) {
    MutableNumericValue prev = null;
    MutableNumericValue nxt = null;
    for (final MutableNumericValue p : previous) {
      if (p.timestamp().compare(RelationalOperator.LT, timestamp)) {
        prev = p;
      } else if (p.timestamp().compare(RelationalOperator.GT, timestamp)) {
        nxt = p;
        break;
      }
    }
    if (nxt == null) {
      nxt = next;
    }
    
    switch (fill_policy.realPolicy()) {
    case PREVIOUS_ONLY:
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      break;
    case PREFER_PREVIOUS:
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      break;
    case NEXT_ONLY:
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      break;
    case PREFER_NEXT:
      if (nxt != null) {
        response.reset(timestamp, nxt);
        return response;
      }
      if (prev != null) {
        response.reset(timestamp, prev);
        return response;
      }
      break;
    case NONE:
      break;
    }
    
    final NumericType fill = fill_policy.fill();
    if (fill == null) {
      response.resetNull(timestamp);
    } else {
      response.reset(timestamp, fill);
    }
    return response;
  }
}

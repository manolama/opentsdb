package net.opentsdb.query.joins;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

public class InnerJoin extends BaseJoin {
    
  public InnerJoin(final BaseHashedJoinSet join) {
    super(join);
    left_iterator = join.left_map == null ? null : join.left_map.iterator();
    if (left_iterator != null) {
      pair = new Pair<TimeSeries, TimeSeries>(null, null);  
      next = new Pair<TimeSeries, TimeSeries>(null, null);
      advance();
    } else {
      pair = null;
      next = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Pair<TimeSeries, TimeSeries> next() {
    pair.setKey(next.getKey());
    pair.setValue(next.getValue());
    advance();
    return pair;
  }
  
  @Override
  protected void advance() {
    while (left_iterator.hasNext() || 
          (left_series != null && left_idx < left_series.size())) {
      // see if there are leftovers in the right array to cross on.
      if (left_series != null && right_series != null && right_idx + 1 < right_series.size()) {
        right_idx++;
        next.setKey(left_series.get(left_idx));
        next.setValue(right_series.get(right_idx));
        return;
      }
      
      // advance if necessary.
      if (left_series == null || left_idx + 1 >= left_series.size()) {
        if (left_iterator.hasNext()) {
          left_iterator.advance();
          left_series = left_iterator.value();
        } else {
          left_series = null;
          continue;
        }
        left_idx = 0;
      }
      
      if (right_series == null) {
        right_series = join.right_map.get(left_iterator.key());
        right_idx = -1;
        if (right_series == null) {
          // no match from left to right, iterate to the next left
          left_series = null;
          continue;
        }
      }
      
      // matched a right series..
      if (right_idx + 1 >= right_series.size()) {
        // inc left_idx and start over
        left_idx++;
        right_idx = -1;
      }
      
      if (left_idx >= left_series.size()) {
        left_series = null;
        // exhausted this series, move to the next.
        continue;
      }
      
      // matched!
      right_idx++;
      next.setKey(left_series.get(left_idx));
      next.setValue(right_series.get(right_idx));
      
      if (left_idx + 1 >= left_series.size() && 
          right_idx + 1 >= right_series.size()) {
        right_series = null;
        right_idx = -1;
      }
      return;
    }
    
    // all done!
    next = null;
  }
}

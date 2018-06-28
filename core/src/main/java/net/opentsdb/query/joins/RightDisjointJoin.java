package net.opentsdb.query.joins;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

public class RightDisjointJoin extends BaseJoin {

  public RightDisjointJoin(final BaseHashedJoinSet join) {
    super(join);
    right_iterator = join.right_map == null ? null : join.right_map.iterator();
    if (right_iterator != null) {
      pair = new Pair<TimeSeries, TimeSeries>(null, null);  
      next = new Pair<TimeSeries, TimeSeries>(null, null);
      advance();
    } else {
      pair = null;
      next = null;
    }
  }
  
  @Override
  protected void advance() {
    while (right_iterator.hasNext() || 
          (right_series != null && right_idx < right_series.size())) {
      if (right_series != null && right_idx + 1 < right_series.size()) {
        right_idx++;
        next.setKey(null);
        next.setValue(right_series.get(right_idx));
        return;
      }
      
      // advance if necessary.
      if (right_series == null || right_idx + 1 >= right_series.size()) {
        if (right_iterator.hasNext()) {
          right_iterator.advance();
          right_series = right_iterator.value();
        } else {
          right_series = null;
          continue;
        }
        right_idx = 0;
      }
      
      if (left_series == null) {
        left_series = join.left_map.get(right_iterator.key());
        if (right_series == null) {
          // no match from right to left, that's what we want
          next.setKey(null);
          next.setValue(right_series.get(right_idx));
          return;
        }
      }
    }
    
    // all done!
    next = null;
  }
}

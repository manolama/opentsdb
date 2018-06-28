package net.opentsdb.query.joins;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

public class LeftDisjointJoin extends BaseJoin {

  public LeftDisjointJoin(final BaseHashedJoinSet join) {
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
  protected void advance() {
    while (left_iterator.hasNext() || 
          (left_series != null && left_idx < left_series.size())) {
      if (left_series != null && left_idx + 1 < left_series.size()) {
        left_idx++;
        next.setKey(left_series.get(left_idx));
        next.setValue(null);
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
          // no match from left to right, that's what we want
          next.setKey(left_series.get(left_idx));
          next.setValue(null);
          return;
        }
      }
    }
    
    // all done!
    next = null;
  }
}

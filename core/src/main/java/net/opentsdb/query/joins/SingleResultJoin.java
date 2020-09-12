package net.opentsdb.query.joins;

import java.util.List;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;

public class SingleResultJoin extends BaseJoin {

  protected SingleResultJoin(final BaseHashedJoinSet join) {
    super(join);
    advance();
  }

  @Override
  protected void advance() {
    final TLongObjectIterator<List<TimeSeries>> iterator;
    List<TimeSeries> series;
    int series_idx;
    
    if (left_iterator != null) {
      iterator = left_iterator;
      series = left_series;
      series_idx = left_idx;
    } else if (right_iterator != null) {
      iterator = right_iterator;
      series = right_series;
      series_idx = right_idx;
    } else {
      // assumption
      iterator = ternary_iterator;
      series = ternary_series;
      series_idx = ternary_idx;
    }
    
    if (series != null && series_idx >= series.size()) {
      // next
      if (!iterator.hasNext()) {
        next = null;
        return;
      }
      
      iterator.advance();
      series = iterator.value();
      series_idx = 0;
    } else if (iterator.hasNext()) {
      iterator.advance();
      series = iterator.value();
      series_idx = 0;
    } else {
      next = null;
      return;
    }
    
    // got a value
    if (left_iterator != null) {
      left_series = series;
      left_idx = series_idx;
      next[0] = series.get(series_idx);
    } else if (right_iterator != null) {
      right_series = series;
      right_idx = series_idx;
      next[1] = series.get(series_idx);
    } else {
      ternary_series = series;
      ternary_idx = series_idx;
      next[2] = series.get(series_idx);
    }
  }

  @Override
  protected void ternaryAdvance() {
    // no-op, we use Advance here for everything.
  }

}

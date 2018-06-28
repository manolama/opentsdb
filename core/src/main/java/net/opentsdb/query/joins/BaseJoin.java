package net.opentsdb.query.joins;

import java.util.Iterator;
import java.util.List;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.set.TLongSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.utils.Pair;

public abstract class BaseJoin implements Iterator<Pair<TimeSeries, TimeSeries>> {
  final HashedJoinSet join;
  
  TLongObjectIterator<List<TimeSeries>> left_iterator;
  List<TimeSeries> left_series;
  int left_idx;
  
  TLongObjectIterator<List<TimeSeries>> right_iterator;
  List<TimeSeries> right_series;
  int right_idx;
  
  TLongSet completed;
  
  Pair<TimeSeries, TimeSeries> pair;
  Pair<TimeSeries, TimeSeries> next;
  
  BaseJoin(final HashedJoinSet join) {
    this.join = join;
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
  
  protected abstract void advance();
}

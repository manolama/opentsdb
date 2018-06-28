package net.opentsdb.query.joins;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinSet;
import net.opentsdb.utils.Pair;

public class HashedJoinSet implements Iterable<Pair<TimeSeries, TimeSeries>> {

  final JoinSet join;
  final String left_key;
  final String right_key;
  
  TLongObjectMap<List<TimeSeries>> left_map;
  TLongObjectMap<List<TimeSeries>> right_map;
  
  HashedJoinSet(final JoinSet join) {
    this.join = join;
    left_key = join.namespaces != null ? 
        join.namespaces.getKey() + join.metrics.getKey() :
          join.metrics.getKey();
    right_key = join.namespaces != null ? 
        join.namespaces.getValue() + join.metrics.getValue() :
          join.metrics.getValue();
  }
  
  void add(String key, long hash, TimeSeries ts) {
    if (key.equals(left_key)) {
      if (left_map == null) {
        left_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = left_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        left_map.put(hash, series);
      }
      series.add(ts);
    } else {
      if (right_map == null) {
        right_map = new TLongObjectHashMap<List<TimeSeries>>();
      }
      List<TimeSeries> series = right_map.get(hash);
      if (series == null) {
        series = Lists.newArrayList();
        right_map.put(hash, series);
      }
      series.add(ts);
    }
  }

  @Override
  public Iterator<Pair<TimeSeries, TimeSeries>> iterator() {
    switch(join.type) {
    case INNER:
    case NATURAL:
    case LEFT:
    case RIGHT:
      return new InnerJoin(this);
    case OUTER:
      return new OuterJoin(this, false);
    case OUTER_DISJOINT:
      return new OuterJoin(this, true);
    case LEFT_DISJOINT:
      return new LeftDisjointJoin(this);
    case RIGHT_DISJOINT:
      return new RightDisjointJoin(this);
      default:
        throw new UnsupportedOperationException("GRR!!");
    }
  }
  
}

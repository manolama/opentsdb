package net.opentsdb.query.joins;

import java.util.Iterator;
import java.util.List;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

public abstract class BaseHashedJoinSet implements Iterable<Pair<TimeSeries, TimeSeries>> {

  final JoinType type;
  TLongObjectMap<List<TimeSeries>> left_map;
  TLongObjectMap<List<TimeSeries>> right_map;
  
  public BaseHashedJoinSet(final JoinType type) {
    this.type = type;
  }
  
  @Override
  public Iterator<Pair<TimeSeries, TimeSeries>> iterator() {
    switch(type) {
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

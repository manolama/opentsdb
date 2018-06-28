package net.opentsdb.query.joins;

import java.util.List;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;

public class SimpleHashedJoinSet extends BaseHashedJoinSet {

  public SimpleHashedJoinSet(JoinType type, final TLongObjectMap<List<TimeSeries>> left, final TLongObjectMap<List<TimeSeries>> right) {
    super(type);
    this.left_map = left;
    this.right_map = right;
  }

}

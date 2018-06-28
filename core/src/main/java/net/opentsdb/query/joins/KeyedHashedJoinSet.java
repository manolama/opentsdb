package net.opentsdb.query.joins;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.Pair;

public class KeyedHashedJoinSet extends BaseHashedJoinSet {
  final String left_key;
  final String right_key;
  
  KeyedHashedJoinSet(final JoinType type, final String left_key, final String right_key) {
    super(type);
    this.left_key = left_key;
    this.right_key = right_key;
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
  
}

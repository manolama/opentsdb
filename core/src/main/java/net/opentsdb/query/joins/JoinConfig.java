package net.opentsdb.query.joins;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.opentsdb.utils.Pair;

public class JoinConfig {

  static enum JoinType {
    /* Present in A and B. Cross product */
    INNER,
    /* Present in A or B. Cross product */
    OUTER,
    /* Present in A xor B. No cross product */
    OUTER_DISJOINT,
    /* Present in A and/or B. Cross product */
    LEFT,
    /* Present in A not B. No cross product */
    LEFT_DISJOINT,
    /* PResent in B and/or A. Cross product */
    RIGHT,
    /* Present in B not A. No cross product */
    RIGHT_DISJOINT,
    /* Full tag join in both. No cross product */
    NATURAL
  }
  
  final DefaultJoin default_join;
  final List<JoinSet> joins;
  final Map<String, List<JoinSet>> nsm_to_join_sets;
  
  public JoinConfig(final DefaultJoin default_join, final List<JoinSet> joins) {
    this.default_join = default_join;
    this.joins = joins;
    nsm_to_join_sets = Maps.newHashMap();
    if (joins != null) {
      for (final JoinSet join : joins) {
        String nm_key = join.namespaces != null ? 
            join.namespaces.getKey() + join.metrics.getKey() :
              join.metrics.getKey();
        
        List<JoinSet> sets = nsm_to_join_sets.get(nm_key);
        if (sets == null) {
          sets = Lists.newArrayList();
          nsm_to_join_sets.put(nm_key, sets);
        }
        if (!sets.contains(join)) {
          sets.add(join);
        }
        
        nm_key = join.namespaces != null ? 
            join.namespaces.getValue() + join.metrics.getValue() :
              join.metrics.getValue();
            
        sets = nsm_to_join_sets.get(nm_key);
        if (sets == null) {
          sets = Lists.newArrayList();
          nsm_to_join_sets.put(nm_key, sets);
        }
        if (!sets.contains(join)) {
          sets.add(join);
        }
      }
    }
  }
  
  public List<JoinSet> joins() {
    return joins;
  }
  
  public Map<String, List<JoinSet>> metric_to_join_sets() {
    return nsm_to_join_sets;
  }
  
  public static class JoinSet {
    JoinType type;
    Pair<String, String> namespaces;
    Pair<String, String> metrics;
    List<Pair<String, String>> joins;
    boolean include_agg_tags;
    boolean include_disjoint_tags;
  }
  
  public static class DefaultJoin {
    JoinType type;
    List<String> tags;
    boolean include_agg_tags;
    boolean include_disjoint_tags;
  }
}

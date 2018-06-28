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
  
  JoinType type;
  List<Pair<String, String>> joins;
  boolean include_agg_tags;
  boolean include_disjoint_tags;
  
  public JoinConfig(final JoinType type, final List<Pair<String, String>> joins) {
    this.type = type;
    this.joins = joins;
  }
  
  public JoinType type() {
    return type;
  }
  
  public List<Pair<String, String>> joins() {
    return joins;
  }
  
}

// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.joins;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.reflect.TypeToken;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.opentsdb.core.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;
import net.opentsdb.utils.Pair;

/**
 * A class that performs a join on time series across multiple groups in the
 * time series source. For each time series, a join key is computed based on the
 * series ID (this the iterators must be initialized before they get here). Then
 * the iterators are sorted by key into a map of maps returned by 
 * {@link #join(IteratorGroups)}. 
 * <p>
 * If the operator is set to {@link SetOperator#UNION} then the map is returned
 * as is and the caller must fill in missing group types with proper values or
 * handle them appropriately.
 * <p>
 * If the operator is set to {@link SetOperator#INTERSECTION} then types and/or
 * entire join keys will be removed if one or more groups are missing iterators.
 * 
 * @since 3.0
 */
public class Joiner {
  private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);
  
  /** A non-null config to pull join information from. */
  final JoinConfig config;
  KeyedHashedJoinSet join_set;
  
  /**
   * Default Ctor.
   * @param config A non-null expression config.
   */
  public Joiner(final JoinConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Join config cannot be null.");
    }
    this.config = config;
  }

  public Iterable<Pair<TimeSeries, TimeSeries>> join(final List<QueryResult> results, 
                   final String left_key, final String right_key) {
    join_set = new KeyedHashedJoinSet(config.type, left_key, right_key);
    
    // TODO - convert byte IDs.
    
    // calculate the hashes for every time series and joins.
    for (final QueryResult result : results) {
      for (final TimeSeries ts : result.timeSeries()) {
        final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
        
        final String key;
        if (Strings.isNullOrEmpty(id.alias())) {
          key = Strings.isNullOrEmpty(id.namespace()) ? 
              id.metric() :
                id.namespace() + id.metric();
        } else {
          key = Strings.isNullOrEmpty(id.namespace()) ? 
              id.alias() :
                id.namespace() + id.alias();
        }
        hash(ts, join_set, key);
      }
    }
    
    return join_set;
  }
  
  void hash(TimeSeries ts,
            KeyedHashedJoinSet join_set, 
            String key) {
    final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    Hasher hasher = Const.HASH_FUNCTION().newHasher();
    final Map<String, String> sorted_tags = id.tags() != null && !id.tags().isEmpty() ? 
        new TreeMap<String, String>(id.tags()) : null;
    
    switch (config.type) {
    case NATURAL:
      // full ID
      if (sorted_tags != null) {
        for (final Entry<String, String> entry : sorted_tags.entrySet()) {
          hasher.putString(entry.getValue(), Const.UTF8_CHARSET);
        }
      }
      break;
      
    default:
      if (config.joins != null) {
        boolean is_left = join_set.left_key.equals(key);
        
        boolean matched = true;
        for (final Pair<String, String> pair : config.joins) {
          String value = id.tags().get(is_left ? pair.getKey() : pair.getValue());
          if (Strings.isNullOrEmpty(value)) {
            // TODO - log the ejection
            matched = false;
            break;
          }
          System.out.println("  Add to hash: " + value);
          hasher.putString(value, Const.UTF8_CHARSET);
        }
        if (!matched) {
          // TODO - log the ejection
          return;
        }
      }
      
    }
    
    if (config.type == JoinType.NATURAL || 
        config.include_agg_tags && id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
      List<String> aggs = Lists.newArrayList(id.aggregatedTags());
      Collections.sort(aggs);
      for (final String agg : aggs) {
        hasher.putString(agg, Const.UTF8_CHARSET);
      }
    }
    
    if (config.type == JoinType.NATURAL ||
        config.include_disjoint_tags && id.disjointTags() != null && !id.disjointTags().isEmpty()) {
      List<String> disj = Lists.newArrayList(id.disjointTags());
      Collections.sort(disj);
      for (final String dis : disj) {
        hasher.putString(dis, Const.UTF8_CHARSET);
      }
    }
    
    System.out.println("HASHING: " + ts.id() + "  TO " + hasher.hash().asLong());
    join_set.add(key, hasher.hash().asLong(), ts);
  }
  
}

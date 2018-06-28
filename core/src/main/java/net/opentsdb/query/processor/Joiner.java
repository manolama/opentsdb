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
package net.opentsdb.query.processor;

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
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.JoinConfig.JoinSet;
import net.opentsdb.query.processor.JoinConfig.JoinType;
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

  public void join(final List<QueryResult> results) {
    List<HashedJoinSet> joins = Lists.newArrayListWithCapacity(config.joins().size());
    Map<String, HashedJoinSet> keyed_joins = Maps.newHashMap(); 
    for (final JoinSet join : config.joins()) {
      final HashedJoinSet hashed = new HashedJoinSet(join);
      joins.add(hashed);
      
      String key = join.namespaces != null ? 
          join.namespaces.getKey() + join.metrics.getKey() :
            join.metrics.getKey();
      keyed_joins.put(key, hashed);
      
      key = join.namespaces != null ? 
          join.namespaces.getValue() + join.metrics.getValue() :
            join.metrics.getValue();
      keyed_joins.put(key, hashed);
    }
    
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
        
        HashedJoinSet join_set = keyed_joins.get(key);
        if (join_set == null) {
          // TODO - log ejection
          continue;
        }
        
        hash(ts, join_set, key);
      }
    }
    
    // TODO figure out join order based on the expression if present
    System.out.println(joins);
    HashedJoinSet hjs = joins.get(0);
    for (final Pair<TimeSeries, TimeSeries> pair : hjs) {
      System.out.println("PAIR: " + 
         (pair.getKey() == null ? "null" : pair.getKey().id().toString()) + 
         ", " + 
         (pair.getValue() == null ? "null" : pair.getValue().id().toString()));
    }
    System.out.println("DONE ITerating");
  }
  
  void hash(TimeSeries ts, 
            HashedJoinSet join_set, 
            String key) {
    final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    Hasher hasher = Const.HASH_FUNCTION().newHasher();
    final Map<String, String> sorted_tags = id.tags() != null && !id.tags().isEmpty() ? 
        new TreeMap<String, String>(id.tags()) : null;
    
    switch (join_set.join.type) {
    case NATURAL:
      // full ID
      if (sorted_tags != null) {
        for (final Entry<String, String> entry : sorted_tags.entrySet()) {
          hasher.putString(entry.getValue(), Const.UTF8_CHARSET);
        }
      }
      break;
      
    default:
      if (join_set.join.joins != null) {
        boolean is_left = join_set.left_key.equals(key);
        
        boolean matched = true;
        for (final Pair<String, String> pair : join_set.join.joins) {
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
    
    if (join_set.join.type == JoinType.NATURAL || 
        join_set.join.include_agg_tags && id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
      List<String> aggs = Lists.newArrayList(id.aggregatedTags());
      Collections.sort(aggs);
      for (final String agg : aggs) {
        hasher.putString(agg, Const.UTF8_CHARSET);
      }
    }
    
    if (join_set.join.type == JoinType.NATURAL ||
        join_set.join.include_disjoint_tags && id.disjointTags() != null && !id.disjointTags().isEmpty()) {
      List<String> disj = Lists.newArrayList(id.disjointTags());
      Collections.sort(disj);
      for (final String dis : disj) {
        hasher.putString(dis, Const.UTF8_CHARSET);
      }
    }
    
    System.out.println("HASHING: " + ts.id() + "  TO " + hasher.hash().asLong());
    join_set.add(key, hasher.hash().asLong(), ts);
  }
  
  class HashedJoinSet implements Iterable<Pair<TimeSeries, TimeSeries>> {
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
        return new InnerJoin();
        default:
          throw new UnsupportedOperationException("GRR!!");
      }
    }
    
    class InnerJoin implements Iterator<Pair<TimeSeries, TimeSeries>> {
      TLongObjectIterator<List<TimeSeries>> left_iterator;
      List<TimeSeries> left_series;
      int left_idx;
      
      List<TimeSeries> right_series;
      int right_idx;
      
      Pair<TimeSeries, TimeSeries> pair;
      Pair<TimeSeries, TimeSeries> next;
      
      InnerJoin() {
        left_iterator = left_map == null ? null : left_map.iterator();
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
      
      void advance() {
        while (left_iterator.hasNext() || 
              (left_series != null && left_idx < left_series.size())) {
          // see if there are leftovers in the right array to cross on.
          if (right_series != null && right_idx + 1 < right_series.size()) {
            right_idx++;
            next.setKey(left_series.get(left_idx));
            next.setValue(right_series.get(right_idx));
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
            right_series = right_map.get(left_iterator.key());
            right_idx = -1;
            if (right_series == null) {
              // no match from left to right, iterate to the next left
              left_series = null;
              continue;
            }
          }
          
          // matched a right series..
          if (right_idx + 1 >= right_series.size()) {
            // inc left_idx and start over
            left_idx++;
            right_idx = -1;
          }
          
          if (left_idx >= left_series.size()) {
            left_series = null;
            // exhausted this series, move to the next.
            continue;
          }
          
          // matched!
          right_idx++;
          next.setKey(left_series.get(left_idx));
          next.setValue(right_series.get(right_idx));
          
          if (left_idx + 1 >= left_series.size() && 
              right_idx + 1 >= right_series.size()) {
            right_series = null;
            right_idx = -1;
          }
          return;
        }
        
        // all done!
        next = null;
      }
      
    }
    
    class FullOuter implements Iterator<Pair<TimeSeries, TimeSeries>> {

      TLongObjectIterator<List<TimeSeries>> left_iterator;
      List<TimeSeries> left_series;
      int left_idx;
      
      TLongObjectIterator<List<TimeSeries>> right_iterator;
      List<TimeSeries> right_series;
      int right_idx;
      TLongSet completed;
      
      Pair<TimeSeries, TimeSeries> pair;
      Pair<TimeSeries, TimeSeries> next;
      
      FullOuter() {
        left_iterator = left_map == null ? null : left_map.iterator();
        right_iterator = right_map == null ? null : right_map.iterator();
        completed = new TLongHashSet();
        if (left_iterator != null || right_iterator != null) {
          pair = new Pair<TimeSeries, TimeSeries>(null, null);  
          next = new Pair<TimeSeries, TimeSeries>(null, null);
          advance();
        } else {
          pair = null;
          next = null;
        }
      }
      
      @Override
      public boolean hasNext() {
        return pair != null;
      }

      @Override
      public Pair<TimeSeries, TimeSeries> next() {
        return pair;
      }
      
      void advance() {
        // exhaust the left hand side first.
        if (left_iterator != null) {
          while (left_iterator.hasNext() || 
              (left_series != null && left_idx < left_series.size())) {
            // see if there are leftovers in the right array to cross on.
            if (right_series != null && right_idx + 1 < right_series.size()) {
              right_idx++;
              next.setKey(left_series.get(left_idx));
              next.setValue(right_series.get(right_idx));
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
            
            // pull out the matching series on the right
            if (right_series == null) {
              right_series = right_map.get(left_iterator.key());
              right_idx = -1;
              if (right_series == null) {
                // no match from left to right, iterate to the next left
                left_series = null;
                continue;
              }
            }
            
            // matched a right series..
            if (right_idx + 1 >= right_series.size()) {
              // inc left_idx and start over
              left_idx++;
              right_idx = -1;
            }
            
            if (left_idx >= left_series.size()) {
              left_series = null;
              completed.add(left_iterator.key());
              // exhausted this series, move to the next.
              continue;
            }
            
            // matched!
            right_idx++;
            next.setKey(left_series.get(left_idx));
            next.setValue(right_series.get(right_idx));
            
            if (left_idx + 1 >= left_series.size() && 
                right_idx + 1 >= right_series.size()) {
              right_series = null;
              right_idx = -1;
            }
            return;
          }
        
          // all done!
          left_iterator = null;
          left_series = null;
          // reset the right to be safe
          right_idx = -1;
          right_series = null;
        } 
        
        // WORK RIGHT SIDE!
        if (right_iterator != null) {
          while (right_iterator.hasNext() || 
              (right_series != null && right_idx < right_series.size())) {
            // see if we have a left series and more to work with.
            if (left_series != null && left_idx + 1 < left_series.size()) {
              left_idx++;
              next.setKey(left_series.get(left_idx));
              next.setValue(right_series.get(right_idx));
              return;
            }
            
            // advance if necessary.
            if (right_series == null || right_idx + 1 >= right_series.size()) {
              if (right_iterator.hasNext()) {
                right_iterator.advance();
                right_series = right_iterator.value();
                // see if this has been processed already.
                // TODO - this is a trade-off between making copies of the
                // source maps where we could delete the entries as we work
                // vs just keeping a copy of the processed hashes and skipping
                // them as we go.
                if (completed.contains(right_iterator.key())) {
                  right_series = null;
                  continue;
                }
              } else {
                right_series = null;
                continue;
              }
              right_idx = 0;
            }
            
            // pull out the matching series on the left
            if (left_series == null) {
              left_series = left_map.get(right_iterator.key());
              left_idx = -1;
              if (left_series == null) {
                // no match from left to right, iterate to the next right
                right_series = null;
                continue;
              }
            }
            
            // matched a right series..
            if (left_idx + 1 >= left_series.size()) {
              // inc right_idx and start over
              right_idx++;
              left_idx = -1;
            }
            
            if (right_idx >= right_series.size()) {
              right_series = null;
              // exhausted this series, move to the next.
              continue;
            }
            
            // matched!
            left_idx++;
            next.setKey(left_series.get(left_idx));
            next.setValue(right_series.get(right_idx));
            
            if (left_idx + 1 >= left_series.size() && 
                right_idx + 1 >= right_series.size()) {
              left_series = null;
              left_idx = -1;
            }
            return;
          }
        }
        
        // all done!
        next = null;
      } 
    }  
  }
  
}

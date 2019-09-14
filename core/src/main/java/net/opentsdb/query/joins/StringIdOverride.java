// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;

/**
 * A simple wrapper for single-sided joins that wraps the source 
 * ID with the proper alias for use as the alias and metric.
 * 
 * @since 3.0
 */
public class StringIdOverride extends BaseTimeSeriesStringId {
  
  /**
   * Default package private ctor.
   * @param id A non-null ID to use as the source.
   * @param alias A non-null alias.
   */
  StringIdOverride(final TimeSeriesStringId id, final String alias) {
    super(BaseTimeSeriesStringId.newBuilder()
        .setAlias(alias)
        .setNamespace(id.namespace())
        .setMetric(id.metric())
        .setTags(id.tags())
        .setAggregatedTags(id.aggregatedTags())
        .setDisjointTags(id.disjointTags())
        .setUniqueId(id.uniqueIds())
        .setHits(id.hits()));
  }
  
}
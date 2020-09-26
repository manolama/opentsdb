// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketpercentile;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.utils.Pair;

public class BucketPercentileFactory extends BaseQueryNodeFactory<BucketPercentileConfig, BucketPercentile> {

  public static final String TYPE = "BucketPercentile";

  @Override
  public BucketPercentileConfig parseConfig(final ObjectMapper mapper, 
                                            final TSDB tsdb, 
                                            final JsonNode node) {
    return BucketPercentileConfig.parse(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final BucketPercentileConfig config,
                         final QueryPlanner plan) {
    final Set<QueryNodeConfig> downstream = plan.configGraph().successors(config);
    BucketPercentileConfig.Builder builder = config.toBuilder();
    
    // TODO - super inefficient walk.. ug.
    for (final QueryNodeConfig ds : downstream) {
      if (builder.underFlowId() == null && !Strings.isNullOrEmpty(config.getUnderFlow())) {
        final String metric = plan.getMetricForDataSource(ds, config.getUnderFlow());
        if (!Strings.isNullOrEmpty(metric)) {
          builder.setUnderFlowId(matchId(ds, config.getUnderFlow()))
                 .setUnderFlowMetric(metric);
        }
      }
      
      if (builder.overFlowId() == null && !Strings.isNullOrEmpty(config.getOverFlow())) {
        final String metric = plan.getMetricForDataSource(ds, config.getOverFlow());
        if (!Strings.isNullOrEmpty(metric)) {
          builder.setOverFlowId(matchId(ds, config.getOverFlow()))
                 .setOverFlowMetric(metric);
        }
      }
      
      for (final String histogram : config.getHistograms()) {
        final String metric = plan.getMetricForDataSource(ds, histogram);
        if (!Strings.isNullOrEmpty(metric)) {
          builder.addHistogramId(matchId(ds, histogram))
                 .addHistogramMetric(metric);
        }
      }
    }
    
    plan.replace(config, builder.build());
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public BucketPercentile newNode(QueryPipelineContext context, BucketPercentileConfig config) {
    return new BucketPercentile(this, context, config);
  }
  
  QueryResultId matchId(final QueryNodeConfig config, final String id) {
    final List<QueryResultId> ids = config.resultIds();
    for (int i = 0; i < ids.size(); i++) {
      final QueryResultId result_id = ids.get(i);
      if (result_id.dataSource().equals(id)) {
        return result_id;
      }
    }
    return null;
  }
}

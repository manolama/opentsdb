// This file is part of OpenTSDB.
// Copyright (C) 2018-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.plan;

import java.util.List;

import com.stumbleupon.async.Deferred;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;

/**
 * A query planner that handles push-down operations to data sources.
 * 
 * TODO - more work and break it into an interface like the old one.
 * 
 * @since 3.0
 */
public class DefaultQueryPlanner extends BaseQueryPlanner {
  
  /**
   * Default ctor.
   * @param context The non-null context to pull the query from.
   * @param context_sink The non-null context pass-through node.
   */
  public DefaultQueryPlanner(final QueryPipelineContext context,
                             final QueryNode context_sink) {
    super(context, context.query(), context_sink);
  }
  
  /**
   * Does the hard work.
   */
  public Deferred<Void> plan(final Span span) {
    buildInitialConfigGraph();
    checkSatisfiedFilters();
    findDataSourceNodes();
    final List<Deferred<Void>> deferreds = initializeConfigNodes();
    
    return Deferred.group(deferreds)
        .addCallback(Deferreds.VOID_GROUP_CB)
        .addCallbackDeferring(new ConfigInitCB());
  }
  
}
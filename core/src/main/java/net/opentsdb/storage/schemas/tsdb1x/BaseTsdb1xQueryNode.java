// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.SourceNode;

/**
 * The base implementation for a QueryNode returned by a 1x Schema implementation.
 * 
 * @since 3.0
 */
public abstract class BaseTsdb1xQueryNode implements TimeSeriesDataSource, SourceNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      BaseTsdb1xQueryNode.class);
  
  private static final Deferred<Void> INITIALIZED = 
      Deferred.fromResult(null);
  
  protected final Tsdb1xDataStoreFactory factory;
  protected final Tsdb1xDataStore store;
  
  /** The pipeline context. */
  protected final QueryPipelineContext context;
  
  /** The upstream query nodes. */
  protected Collection<QueryNode> upstream;
  
  /** The downstream query nodes. */
  protected Collection<QueryNode> downstream;
  
  /** The downstream source nodes. */
  protected Collection<TimeSeriesDataSource> downstream_sources;
  
  /** The query source config. */
  protected final TimeSeriesDataSourceConfig config;
  
  /** Rollup fallback mode. */
  protected final RollupUsage rollup_usage;
  
  /** Rollup intervals matching the query downsampler if applicable. */
  protected List<RollupInterval> rollup_intervals;
  
  /** When we start fetching data. */
  protected long fetch_start;
  
  public BaseTsdb1xQueryNode(final Tsdb1xDataStoreFactory factory,
                             final Tsdb1xDataStore store,
                             final QueryPipelineContext context,
                             final TimeSeriesDataSourceConfig config) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    if (context.tsdb().getConfig() == null) {
      throw new IllegalArgumentException("Can't execute a query without "
          + "a configuration in the source config!");
    }
    this.factory = factory;
    this.store = store;
    this.context = context;
    this.config = config;
    
    rollup_usage = RollupUsage.parse(factory.tsdb().getConfig().getString(
        store.getConfigKey(Tsdb1xDataStore.ROLLUP_USAGE_KEY)));
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  public List<RollupInterval> rollupIntervals() {
    return rollup_intervals;
  }
  
  public RollupUsage rollupUsage() {
    return rollup_usage;
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass() + ".initialize()").start();
    } else {
      child = null;
    }

    if (store.schema().rollupConfig() != null && 
        rollup_usage != RollupUsage.ROLLUP_RAW &&
        config.getRollupIntervals() != null && 
        !config.getRollupIntervals().isEmpty()) {
      rollup_intervals = Lists.newArrayListWithExpectedSize(
          config.getRollupIntervals().size());
      for (final String interval : config.getRollupIntervals()) {
        final RollupInterval ri = store.schema().rollupConfig()
            .getRollupInterval(interval);
        if (ri != null) {
          rollup_intervals.add(ri);
        }
      }
    } else {
      rollup_intervals = null;
    }
    
    upstream = context.upstream(this);
    downstream = context.downstream(this);
    downstream_sources = context.downstreamSources(this);
    if (child != null) {
      child.setSuccessTags().finish();
    }
    return INITIALIZED;
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return context;
  }
  
  @Override
  public Schema schema() {
    return store.schema();
  }
  
  public TSDB tsdb() {
    return factory.tsdb();
  }
  
  public Tsdb1xDataStore store() {
    return store;
  }
  
  public String getConfigKey(final String suffix) {
    return store.getConfigKey(suffix);
  }
  
  public boolean fetchDataType(final byte type) {
    // TODO
    return true;
  }
  
  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      @Override
      public void run() {
        completeUpstream(final_sequence, total_sequences);
      }
    });
  }
  
  @Override
  public void onNext(final PartialTimeSeries series) {
    throw new IllegalArgumentException("Not implemented yet.");
  }

  @Override
  public void onError(final Throwable t) {
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      @Override
      public void run() {
        sendUpstream(t);        
      }
    });
  }
  
  /**
   * Sends the result to each of the upstream subscribers.
   * 
   * @param result A non-null result.
   * @throws QueryUpstreamException if the upstream 
   * {@link #onNext(QueryResult)} handler throws an exception. I hate
   * checked exceptions but each node needs to be able to handle this
   * ideally by cancelling the query.
   * @throws IllegalArgumentException if the result was null.
   */
  protected void sendUpstream(final QueryResult result) 
        throws QueryUpstreamException {
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onNext(result);
      } catch (Exception e) {
        throw new QueryUpstreamException("Failed to send results "
            + "upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Sends the throwable upstream to each of the subscribing nodes. If 
   * one or more upstream consumers throw an exception, it's caught and
   * logged as a warning.
   * 
   * @param t A non-null throwable.
   * @throws IllegalArgumentException if the throwable was null.
   */
  protected void sendUpstream(final Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Throwable cannot be null.");
    }
    
    for (final QueryNode node : upstream) {
      try {
        node.onError(t);
      } catch (Exception e) {
        LOG.warn("Failed to send exception upstream to node: " + node, e);
      }
    }
  }
  
  /**
   * Passes the sequence info upstream to all subscribers. If one or 
   * more upstream consumers throw an exception, it's caught and logged 
   * as a warning.
   * 
   * @param final_sequence The final sequence number to pass.
   * @param total_sequences The total sequence count to pass.
   */
  protected void completeUpstream(final long final_sequence,
                                  final long total_sequences) {
    for (final QueryNode node : upstream) {
      try {
        node.onComplete(this, final_sequence, total_sequences);
      } catch (Exception e) {
        LOG.warn("Failed to mark upstream node complete: " + node, e);
      }
    }
  }
  
}
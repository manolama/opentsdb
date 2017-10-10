// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.SlicedTimeSeries;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard2;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.QueryExecutor2;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.GroupBy;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A simple store that generates a set of time series to query as well as stores
 * new values (must be written in time order) all in memory. It's meant for
 * testing pipelines and benchmarking.
 * 
 * @since 3.0
 */
public class MockDataStore extends TimeSeriesDataStore implements QueryExecutor2, QueryNodeFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MockDataStore.class);
  
  public static final long ROW_WIDTH = 3600000;
  public static final long HOSTS = 4;
  public static final long INTERVAL = 60000;
  public static final long HOURS = 24;
  public static final List<String> DATACENTERS = Lists.newArrayList(
      "PHX", "LGA", "LAX", "DEN");
  public static final List<String> METRICS = Lists.newArrayList(
      "sys.cpu.user", "sys.if.out", "sys.if.in", "web.requests");

  /** The super inefficient and thread unsafe in-memory db. */
  private Map<TimeSeriesId, MockSpan> database;
  
  private ExecutorService thread_pool;
  
  @Override
  public QueryNode newNode(QueryPipelineContext context,
      QueryNodeConfig config) {
    return new MyPipeline(context, (MDSConfig) config);
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    database = Maps.newHashMap();
    generateMockData(tsdb);
    if (tsdb.getConfig().hasProperty("MockDataStore.threadpool.enable") && 
        tsdb.getConfig().getBoolean("MockDataStore.threadpool.enable")) {
      thread_pool = Executors.newCachedThreadPool();
      System.out.println("INITed the thread pool");
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> shutdown() {
    if (thread_pool != null) {
      thread_pool.shutdownNow();
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> write(final TimeSeriesId id,
                                final TimeSeriesValue<?> value, 
                                final TsdbTrace trace,
                                final Span upstream_span) {    
    MockSpan span = database.get(id);
    if (span == null) {
      span = new MockSpan(id);
      database.put(id, span);
    }
    
    span.addValue(value);
    return Deferred.fromResult(null);
  }
  
  @Override
  public String id() {
    return "MockDataStore";
  }

  @Override
  public String version() {
    return "0.0.0";
  }

  class MockSpan {
    private List<MockRow> rows = Lists.newArrayList();
    private final TimeSeriesId id;
    
    public MockSpan(final TimeSeriesId id) {
      this.id = id;
    }
    
    public void addValue(TimeSeriesValue<?> value) {
      
      long base_time = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      
      for (final MockRow row : rows) {
        if (row.base_timestamp == base_time) {
          row.addValue(value);
          return;
        }
      }
      
      final MockRow row = new MockRow(id, value);
      rows.add(row);
    }
  
    List<MockRow> rows() {
      return rows;
    }
  }
  
  class MockRow implements TimeSeries {
    private TimeSeriesId id;
    public long base_timestamp;
    public Map<TypeToken<?>, TimeSeriesDataSource> sources;
    
    public MockRow(final TimeSeriesId id, 
                   final TimeSeriesValue<?> value) {
      this.id = id;
      base_timestamp = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      sources = Maps.newHashMap();
      // TODO - other types
      if (value.type() == NumericType.TYPE) {
        sources.put(NumericType.TYPE, new NumericMillisecondShard2( 
            new MillisecondTimeStamp(base_timestamp), 
            new MillisecondTimeStamp(base_timestamp + ROW_WIDTH)));
        addValue(value);
      }
    }
    
    public void addValue(final TimeSeriesValue<?> value) {
      // TODO - other types
      if (value.type() == NumericType.TYPE) {
        NumericMillisecondShard2 shard = 
            (NumericMillisecondShard2) sources.get(NumericType.TYPE);
        if (shard == null) {
          shard = new NumericMillisecondShard2( 
              new MillisecondTimeStamp(base_timestamp), 
              new MillisecondTimeStamp(base_timestamp + ROW_WIDTH));
          sources.put(NumericType.TYPE, shard);
        }
        if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
          shard.add(value.timestamp().msEpoch(), 
              ((TimeSeriesValue<NumericType>) value).value().longValue());
        } else {
          shard.add(value.timestamp().msEpoch(), 
              ((TimeSeriesValue<NumericType>) value).value().doubleValue());
        }
      }
    }

    @Override
    public TimeSeriesId id() {
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      // TODO - other types
      if (type == NumericType.TYPE) {
        return Optional.of(((NumericMillisecondShard2) 
            sources.get(NumericType.TYPE)).iterator());
      }
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      // TODO - other types
      final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> its = 
          Lists.newArrayListWithCapacity(1);
      its.add(((NumericMillisecondShard2) sources.get(NumericType.TYPE)).iterator());
      return its;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return sources.keySet();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
    }
    
  }
  
  private void generateMockData(final TSDB tsdb) {
    long start_timestamp = DateTime.currentTimeMillis() - 2 * ROW_WIDTH;
    start_timestamp = start_timestamp - start_timestamp % ROW_WIDTH;
    if (tsdb.getConfig().hasProperty("MockDataStore.timestamp")) {
      start_timestamp = tsdb.getConfig().getLong("MockDataStore.timestamp");
    }
    
    long hours = HOURS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hours")) {
      hours = tsdb.getConfig().getLong("MockDataStore.hours");
    }
    
    long hosts = HOSTS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hosts")) {
      hosts = tsdb.getConfig().getLong("MockDataStore.hosts");
    }
    
    long interval = INTERVAL;
    if (tsdb.getConfig().hasProperty("MockDataStore.interval")) {
      interval = tsdb.getConfig().getLong("MockDataStore.interval");
      if (interval <= 0) {
        throw new IllegalStateException("Interval can't be 0 or less.");
      }
    }
    
    for (int t = 0; t < hours; t++) {
      for (final String metric : METRICS) {
        for (final String dc : DATACENTERS) {
          for (int h = 0; h < hosts; h++) {
            TimeSeriesId id = BaseTimeSeriesId.newBuilder()
                .setMetric(metric)
                .addTags("dc", dc)
                .addTags("host", String.format("web%02d", h + 1))
                .build();
            MutableNumericType dp = new MutableNumericType();
            TimeStamp ts = new MillisecondTimeStamp(0);
            for (long i = 0; i < (ROW_WIDTH / interval); i++) {
              ts.updateMsEpoch(start_timestamp + (i * interval) + (t * ROW_WIDTH));
              dp.reset(ts, t + h + i);
              write(id, dp, null, null);
            }
          }
        }

      }
    }
  }

  Map<TimeSeriesId, MockSpan> getDatabase() {
    return database;
  }

  @Override
  public QueryNode executeQuery(final QueryPipelineContext context) {
    return new MyPipeline(context, null);
  }
  
  class MyPipeline extends AbstractQueryNode {
    int[] sequence_ids;
    AtomicBoolean completed = new AtomicBoolean();
    MDSConfig config;
    
    public MyPipeline(final QueryPipelineContext context, MDSConfig config) {
      super(context);
      //listener = context.getListener();
      sequence_ids = new int[1];
      this.config = config;
    }
    
    @Override
    public void fetchNext() {
      try {
        if (context.getContext().mode() == QueryMode.SINGLE && sequence_ids[0] > 0/* && parallel_id >= context.parallelQueries()*/) {
          for (final QueryListener node : upstream) {
            node.onComplete();
          }
          return;
        }
        
        //System.out.println("QUERIES: " + context.parallelQueries() + " PID: " + parallel_id + " SID: " + sequence_ids[parallel_id]);
        LocalResult result;
        synchronized(this) {
          result = new LocalResult(context, this, config, sequence_ids[0]++);
        }

        thread_pool.submit(result);
//        switch(context.getContext().mode()) {
//        case SINGLE:
//          thread_pool.submit(result);
//          break;
//        case CLIENT_STREAM:
//        case CLIENT_STREAM_PARALLEL:
//          thread_pool.submit(result);
//          break;
//        case SERVER_SYNC_STREAM:
//        case SERVER_SYNC_STREAM_PARALLEL:
//          thread_pool.submit(result);
//          break;
//        case SERVER_ASYNC_STREAM:
//          thread_pool.submit(result);
//        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public String id() {
      return config.id();
    }
    
    @Override
    public void close() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing pipeline.");
      }
      for (final QueryListener node : upstream) {
        node.onComplete();
      }
    }

    @Override
    public QueryPipelineContext context() {
      return context;
    }
    
    Collection<QueryListener> upstream() {
      return upstream;
    }
    
    @Override
    public void onComplete() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onNext(QueryResult next) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onError(Throwable t) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public QueryNodeConfig config() {
      return config;
    }
  }
  
  class LocalResult implements QueryResult, Runnable {
    final QueryPipelineContext context;
    final MyPipeline pipeline;
    final int sequence_id;
    final List<TimeSeries> matched_series;
    final MDSConfig config;
    
    LocalResult(QueryPipelineContext context, MyPipeline pipeline, MDSConfig config, int sequence_id) {
      this.context = context;
      this.pipeline = pipeline;
      this.sequence_id = sequence_id;
      this.config = config;
      matched_series = Lists.newArrayList();
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return matched_series;
    }
    
    @Override
    public int sequenceId() {
      return sequence_id;
    }
    
    @Override
    public void run() {
      try {
      long start_ts = context.getContext().mode() == QueryMode.SINGLE ? 
          config.query.getTime().startTime().msEpoch() : 
            config.query.getTime().endTime().msEpoch() - ((sequence_id + 1) * ROW_WIDTH);
      long end_ts = context.getContext().mode() == QueryMode.SINGLE ? 
          config.query.getTime().endTime().msEpoch() : 
            config.query.getTime().endTime().msEpoch() - (sequence_id * ROW_WIDTH);
      
      //System.out.println("START: " + start_ts + "  END: " + end_ts);
      if (end_ts <= config.query.getTime().startTime().msEpoch()) {
        System.out.println("Over the end time. done");
        if (pipeline.completed.compareAndSet(false, true)) {
          for (QueryListener node : pipeline.upstream()) {
            node.onComplete();
          }
        }
        return;
      }
      System.out.println("Running the filter: " + config.query);
      for (final Entry<TimeSeriesId, MockSpan> entry : database.entrySet()) {
        //for (Metric m : config.query.getMetrics()) {
        Metric m = config.metric;
          if (!m.getMetric().equals(entry.getKey().metric())) {
            continue;
          }
          
          if (!Strings.isNullOrEmpty(m.getFilter())) {
            Filter f = null;
            for (Filter filter : config.query.getFilters()) {
              if (filter.getId().equals(m.getFilter())) {
                f = filter;
                break;
              }
            }
            
            if (f == null) {
              // WTF? Shouldn't happen at this level.
              continue;
            }
            
            boolean matched = true;
            for (final TagVFilter tf : f.getTags()) {
              String tagv = entry.getKey().tags().get(tf.getTagk());
              if (tagv == null) {
                matched = false;
                break;
              }
              
              try {
                if (!tf.match(ImmutableMap.of(tf.getTagk(), tagv)).join()) {
                  matched = false;
                  break;
                }
              } catch (Exception e) {
                throw new RuntimeException("WTF?", e);
              }
            }
            
            if (!matched) {
              continue;
            }
          }
          
          // matched the filters
          TimeSeries iterator = context.getContext().mode() == QueryMode.SINGLE ? new SlicedTimeSeries() : null;
          int rows = 0;
          for (final MockRow row : entry.getValue().rows) {
            if (row.base_timestamp >= start_ts && 
                row.base_timestamp < end_ts) {
              ++rows;
              if (context.getContext().mode() == QueryMode.SINGLE) {
                ((SlicedTimeSeries) iterator).addSource(row);  
              } else {
                iterator = row;
                break;
              }
            }
          }
          
          if (rows > 0) {
            matched_series.add(iterator);
          }
        //}
      }
      
      System.out.println("DONE with filtering.");
      
      Collection<QueryListener> listeners = pipeline.upstream();
      System.out.println("LISTNERS: " + listeners);
      for (QueryListener node : pipeline.upstream()) {
        if (matched_series.isEmpty()) {
          System.out.println("Nothing matched, done.");
          node.onComplete();
        } else {
          System.out.println("Sending upstream...");
          node.onNext(this);
        }
      }
      
      switch(context.getContext().mode()) {
      case SINGLE:
      case CLIENT_STREAM:
      case CLIENT_STREAM_PARALLEL:
        break;
      case SERVER_SYNC_STREAM:
      case SERVER_SYNC_STREAM_PARALLEL:
      case SERVER_ASYNC_STREAM:
        pipeline.fetchNext();
      }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public QueryNode source() {
      return pipeline;
    }
  }

  @Override
  public Deferred<Object> close() {
    return Deferred.fromResult(null);
  }

  @Override
  public Collection<QueryNode> outstandingPipelines() {
    // TODO Auto-generated method stub
    return null;
  }


  public static class MDSConfig implements QueryNodeConfig {
    public net.opentsdb.query.pojo.TimeSeriesQuery query;
    public Filter filter;
    public Metric metric;
    
    @Override
    public String id() {
      return metric.getId();
    }
  }
}

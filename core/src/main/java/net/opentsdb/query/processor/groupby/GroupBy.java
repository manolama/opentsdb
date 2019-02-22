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
package net.opentsdb.query.processor.groupby;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.ResultShard;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.Allocator;
import net.opentsdb.pools.CloseablePoolable;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolConfig;
import net.opentsdb.pools.ObjectPoolFactory;
import net.opentsdb.pools.Poolable;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolatorContainer;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolatorContainerPool;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.groupby.GroupByNumericPTS.GroupByNumericPTSPool;
import net.opentsdb.utils.Pair;

/**
 * Performs the time series grouping aggregation by sorting time series according
 * to tag keys and merging the results into single time series using an 
 * aggregation function.
 * <p>
 * For each result returned to {@link #onNext(QueryResult)}, a new set of time 
 * series is generated containing a collection of source time series from the 
 * incoming result set. The actual arithmetic is performed when upstream sources
 * fetch an iterator and being the iteration.
 * 
 * @since 3.0
 */
public class GroupBy extends AbstractQueryNode {
  
  /** The config for this group by node. */
  private final GroupByConfig config;
  
  private List<GBShard> shards;
  
  /**
   * Default ctor.
   * @param factory The non-null factory for generating iterators.
   * @param context The non-null pipeline context we belong to.
   * @param config A non-null group by config to configure the iterators with.
   */
  public GroupBy(final QueryNodeFactory factory, 
                 final QueryPipelineContext context, 
                 final GroupByConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Group By config cannot be null.");
    }
    this.config = config;
  }
    
  @Override
  public void close() {
    // No-op
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (next.idType() == Const.TS_BYTE_ID && 
        config.getEncodedTagKeys() == null &&
        config.getTagKeys() != null && 
        !config.getTagKeys().isEmpty()) {
      
      class ResolveCB implements Callback<Object, List<byte[]>> {
        @Override
        public Object call(List<byte[]> arg) throws Exception {
          synchronized (GroupBy.this) {
            config.setEncodedTagKeys(arg);
          }
          try {
            final GroupByResult result = new GroupByResult(GroupBy.this, next);
            sendUpstream(result);
          } catch (Exception e) {
            sendUpstream(e);
          }
          return null;
        }
      }
      
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          sendUpstream(ex);
          return null;
        }
      }
      
      final Iterator<TimeSeries> iterator = next.timeSeries().iterator();
      if (iterator.hasNext()) {
        final TimeSeriesDataSourceFactory store = ((TimeSeriesByteId) 
            iterator.next().id()).dataStore();
        if (store == null) {
          throw new RuntimeException("The data store was null for a byte series!");
        }
        store.encodeJoinKeys(Lists.newArrayList(config.getTagKeys()), null /* TODO */)
          .addCallback(new ResolveCB())
          .addErrback(new ErrorCB());
      } else {
        final GroupByResult result = new GroupByResult(this, next);
        sendUpstream(result);
      }
    } else {
      final GroupByResult result = new GroupByResult(this, next);
      sendUpstream(result);
    }
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  /** @return The number of upstream consumers. */
  protected int upstreams() {
    return upstream.size();
  }

  @Override
  public void push(PartialTimeSeries series) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void complete(ResultShard shard) {
    // TODO Auto-generated method stub
    
  }
  
  class GBShard implements ResultShard {

    @Override
    public int totalShards() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public QueryNode node() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String dataSource() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeStamp start() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeStamp end() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TimeSeriesId id(long hash) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int timeSeriesCount() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String sourceId() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  
  static class GBNContainer implements CloseablePoolable {
    TLongObjectMap<GBNumericTs> series;
    TLongObjectMap<PartialNumericInterpolatorContainer> interpolators;
    GroupBy gb;
    Poolable poolable;
    
    void completed(ResultShard shard) {
      // This shard is done so we can run through and group the series for it.
    }
    
    void addSeries(final PartialTimeSeries series) {
      // TODO - nanos
      GBNumericTs gb_ts = this.series.get(series.shard().start().msEpoch());
      PartialNumericInterpolatorContainer interpolator = null;
      if (gb_ts == null) {
        // claim
        gb_ts = (GBNumericTs) gb.pipelineContext().tsdb().getRegistry().getObjectPool(TSPool.TYPE).claim().object();
        gb_ts.reset(gb, series.shard());
        this.series.put(series.shard().start().msEpoch(), gb_ts);
        
        interpolator = (PartialNumericInterpolatorContainer)
            gb.pipelineContext().tsdb().getRegistry().getObjectPool(
                PartialNumericInterpolatorContainerPool.TYPE).claim().object();
        interpolators.put(series.idHash(), interpolator);
      } else {
        interpolator = interpolators.get(series.idHash());
        interpolator.newPartial(series);
      }
      gb_ts.addSeries(series);
    }
    
    @Override
    public void close() throws Exception {
      TLongObjectIterator<GBNumericTs> series_it = series.iterator();
      while (series_it.hasNext()) {
        series_it.value().close();
        series_it.advance();
      }
      series.clear();
      
      TLongObjectIterator<PartialNumericInterpolatorContainer> it = interpolators.iterator();
      while (it.hasNext()) {
        it.value().close();
        it.advance();
      }
    }

    @Override
    public void setPoolable(Poolable poolable) {
      this.poolable = poolable;
    }
  }
  
  static interface GBTypedPTS extends PartialTimeSeries, CloseablePoolable {
    void addSeries(final PartialTimeSeries series);
  }
  
  // handles one segment
  static class GBNumericTs implements PartialTimeSeries, CloseablePoolable {
    Map<TypeToken<? extends TimeSeriesDataType>, GBTypedPTS> series;
    // TODO - compute ID
    ResultShard shard;
    GroupBy gb;
    Poolable poolable;
    
    GBNumericTs() {
      series = Maps.newHashMap();
    }
    
    void reset(final GroupBy gb, ResultShard shard) {
      this.gb = gb;
      this.shard = shard;
    }
    
    void addSeries(final PartialTimeSeries series) {
      GBTypedPTS pts = this.series.get(series.getType());
      if (pts == null) {
        pts = (GBTypedPTS) gb.pipelineContext().tsdb().getRegistry().getObjectPool(GroupByNumericPTSPool.TYPE).claim().object();
        this.series.put(series.getType(), pts);
      } else {
        pts.addSeries(series);
      }
    }
    
    @Override
    public void close() throws Exception {
      // TODO - No need to close series as the upstream handlers will do so? URGG Ref counting!
      series.clear();
    }

    @Override
    public long idHash() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public ResultShard shard() {
      return shard;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }

    @Override
    public TypedTimeSeriesIterator iterator() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setPoolable(Poolable poolable) {
      this.poolable = poolable;
    }
    
    public GroupBy node() {
      return gb;
    }
    
  }

  public static class TSPool implements Allocator {
    public static final String TYPE = "GBNumericTs";
    private static final TypeToken<?> TYPE_TOKEN = 
        TypeToken.of(PartialNumericInterpolatorContainer.class);
    private int length;
    private String id;
    private int size;
    private TSDB tsdb;
    
    @Override
    public String type() {
      return TYPE;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {
      this.tsdb = tsdb;
      if (Strings.isNullOrEmpty(id)) {
        this.id = TYPE;
      } else {
        this.id = id;
      }
      
      final ObjectPoolFactory factory = 
          tsdb.getRegistry().getPlugin(ObjectPoolFactory.class, null);
      if (factory == null) {
        return Deferred.fromError(new RuntimeException("No default pool factory found."));
      }
      
      final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
          .setAllocator(this)
          .setInitialCount(4096) // TODO
          .setId(this.id)
          .build();
      
      final ObjectPool pool = factory.newPool(config);
      if (pool != null) {
        tsdb.getRegistry().registerObjectPool(pool);
      } else {
        return Deferred.fromError(new RuntimeException("Null pool returned for: " + id));
      }
      return Deferred.fromResult(null);
    }

    @Override
    public Deferred<Object> shutdown() {
      return Deferred.fromResult(null);
    }

    @Override
    public String version() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int size() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Object allocate() {
      return new GBNumericTs();
    }

    @Override
    public void deallocate(Object object) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public TypeToken<?> dataType() {
      // TODO Auto-generated method stub
      return TYPE_TOKEN;
    }

  }
}

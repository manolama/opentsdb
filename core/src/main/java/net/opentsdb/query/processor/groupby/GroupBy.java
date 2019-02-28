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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.ResultShard;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
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
import net.opentsdb.query.interpolation.PartialQueryInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolatorContainer;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolatorContainerPool;
import net.opentsdb.query.processor.groupby.GroupByConfig;
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
  private static final Logger LOG = LoggerFactory.getLogger(GroupBy.class);
  
  /** The config for this group by node. */
  private final GroupByConfig config;
  
  private TLongObjectMap<GBShard> shards;
  
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
    shards = new TLongObjectHashMap<GBShard>();
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
    GBShard shard = shards.get(series.shard().start().msEpoch());
    if (shard == null) {
      shard = new GBShard(series.shard());
      shards.put(series.shard().start().msEpoch(), shard);
    }
    shard.addSeries(series);
  }

  @Override
  public void complete(ResultShard shard) {
    GBShard gb_shard = shards.get(shard.start().msEpoch());
    gb_shard.completed(shard);
  }
  
  class GBShard implements ResultShard {

    TLongObjectMap<GBNContainer> groups;
    TLongObjectMap<MergedTimeSeriesId.Builder> ids;
    ResultShard shard;
    GBShard(ResultShard shard) {
      groups = new TLongObjectHashMap<GBNContainer>();
      this.shard = shard;
      ids = new TLongObjectHashMap<MergedTimeSeriesId.Builder>();
    }
    
    @Override
    public int totalShards() {
      return shard.totalShards();
    }

    @Override
    public QueryNode node() {
      return GroupBy.this;
    }

    @Override
    public String dataSource() {
      return shard.dataSource();
    }

    @Override
    public TimeStamp start() {
      return shard.start();
    }

    @Override
    public TimeStamp end() {
      return shard.end();
    }

    @Override
    public TimeSeriesId id(long hash) {
      // TODO Auto-generated method stub
      return ids.get(hash).build();
    }

    @Override
    public int timeSeriesCount() {
      return groups.size();
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
    
    void addSeries(final PartialTimeSeries series) {
      TimeSeriesId raw_id = series.shard().id(series.idHash());
      
      boolean matched = true;
      if (raw_id.type().equals(Const.TS_STRING_ID)) {
        final TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
        final StringBuilder buf = new StringBuilder()
            .append(id.metric());
        
        if (!((GroupByConfig) config).getTagKeys().isEmpty()) {

          for (final String key : ((GroupByConfig) config).getTagKeys()) {
            final String tagv = id.tags().get(key);
            if (tagv == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Dropping series from group by due to missing tag key: " 
                    + key + "  " + id);
              }
              matched = false;
              break;
            }
            buf.append(tagv);
          }
        }
        
        if (matched) {
          final long hash = LongHashFunction.xx_r39().hashChars(buf.toString());
          GBNContainer container = groups.get(hash);
          if (container == null) {
            container = new GBNContainer(hash);
            container.gb = GroupBy.this;
            groups.put(hash, container);
          }
          container.addSeries(series);
          
          MergedTimeSeriesId.Builder merging_id = ids.get(hash);
          if (merging_id == null) {
            merging_id = MergedTimeSeriesId.newBuilder();
            ids.put(hash, merging_id);
          }
          merging_id.addSeries(id);
        }
      }
      // TODO - byte arrays
    }

    void completed(ResultShard shard) {
      for (final GBNContainer container : groups.valueCollection()) {
        container.completed(shard);
      }
    }
    
    class GBNContainer implements CloseablePoolable {
      TLongObjectMap<GBNumericTs> series;
      TLongObjectMap<TLongObjectMap<PartialNumericInterpolatorContainer>> interpolators;
      GroupBy gb;
      Poolable poolable;
      long hash;
      
      GBNContainer(long hash) {
        series = new TLongObjectHashMap<GBNumericTs>();
        interpolators = new TLongObjectHashMap<TLongObjectMap<PartialNumericInterpolatorContainer>>();
        this.hash = hash;
      }
      
      void completed(ResultShard shard) {
        // This shard is done so we can run through and group the series for it.
        gb.sendUpstream(series.get(shard.start().msEpoch()));
        gb.completeUpstream(GBShard.this);
      }
      
      void addSeries(final PartialTimeSeries series) {
        // TODO - nanos
        GBNumericTs gb_ts = this.series.get(series.shard().start().msEpoch());
        PartialNumericInterpolatorContainer interpolator = null;
        if (gb_ts == null) {
          // claim
          gb_ts = (GBNumericTs) gb.pipelineContext().tsdb().getRegistry().getObjectPool(TSPool.TYPE).claim().object();
          gb_ts.reset(gb, GBShard.this, hash);
          //System.out.println("******* RESET THE NDOE");
          this.series.put(series.shard().start().msEpoch(), gb_ts);
          
          interpolator = (PartialNumericInterpolatorContainer)
              gb.pipelineContext().tsdb().getRegistry().getObjectPool(
                  PartialNumericInterpolatorContainerPool.TYPE).claim().object();
          TLongObjectMap<PartialNumericInterpolatorContainer> interps = new TLongObjectHashMap<PartialNumericInterpolatorContainer>();
          interpolators.put(series.shard().start().msEpoch(), interps);
          interps.put(series.idHash(), interpolator);
        } else {
          TLongObjectMap<PartialNumericInterpolatorContainer> interps = interpolators.get(series.shard().start().msEpoch());
          interpolator = interps.get(series.idHash());
          if (interpolator == null) {
            interpolator = (PartialNumericInterpolatorContainer)
                gb.pipelineContext().tsdb().getRegistry().getObjectPool(
                    PartialNumericInterpolatorContainerPool.TYPE).claim().object();
            interps.put(series.idHash(), interpolator);
          }
        }
        gb_ts.addSeries(series, interpolator.newPartial(series));
      }
      
      @Override
      public void close() throws Exception {
        TLongObjectIterator<GBNumericTs> series_it = series.iterator();
        while (series_it.hasNext()) {
          series_it.value().close();
          series_it.advance();
        }
        series.clear();
        
        TLongObjectIterator<TLongObjectMap<PartialNumericInterpolatorContainer>> out_it = interpolators.iterator();
        while (out_it.hasNext()) {
          TLongObjectIterator<PartialNumericInterpolatorContainer> in_it = out_it.value().iterator();
          while (in_it.hasNext()) {
            in_it.value().close();
            in_it.advance();
          }
          out_it.advance();
        }
      }

      @Override
      public void setPoolable(Poolable poolable) {
        this.poolable = poolable;
      }
    }
  }
  
  
  
  static interface GBTypedPTS extends PartialTimeSeries, CloseablePoolable {
    void addSeries(final PartialTimeSeries series, PartialQueryInterpolator<NumericType> interp);
    void setSomething(final GBNumericTs node);
  }
  
  // handles one segment
  static class GBNumericTs implements PartialTimeSeries, CloseablePoolable {
    Map<TypeToken<? extends TimeSeriesDataType>, GBTypedPTS> series;
    // TODO - compute ID
    ResultShard shard;
    GroupBy gb;
    Poolable poolable;
    GBTypedPTS pts;
    long hash;
    
    GBNumericTs() {
      series = Maps.newHashMap();
    }
    
    void reset(final GroupBy gb, ResultShard shard, long hash) {
      this.gb = gb;
      this.shard = shard;
      pts = null;
      this.hash = hash;
    }
    
    void addSeries(final PartialTimeSeries series, PartialQueryInterpolator<NumericType> interp) {
      if (pts == null) {
        pts = this.series.get(series.getType());
      }
      if (pts == null) {
        pts = (GBTypedPTS) ((GroupByFactory) gb.factory()).getIterator(series.getType());
        pts.setSomething(this);
        this.series.put(series.getType(), pts);
      } else {
        pts.addSeries(series, interp);
      }
      try {
        series.close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } 
    }
    
    @Override
    public void close() throws Exception {
      // TODO - No need to close series as the upstream handlers will do so? URGG Ref counting!
      series.clear();
    }

    @Override
    public long idHash() {
      return hash;
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
      return pts.iterator();
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

// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.ResultShard;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.pools.Allocator;
import net.opentsdb.pools.CloseablePoolable;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.MutableNumericValuePool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolConfig;
import net.opentsdb.pools.ObjectPoolFactory;
import net.opentsdb.pools.Poolable;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.PartialNumericInterpolatorContainer;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.processor.groupby.GroupBy.GBNumericTs;
import net.opentsdb.query.processor.groupby.GroupBy.GBTypedPTS;
import net.opentsdb.query.processor.groupby.GroupByConfig;

/**
 * An iterator for group-by operations wherein multiple time series are 
 * aggregated into a single time series using an aggregation function and 
 * interpolators to fill missing or unaligned values.
 * <p>
 * Since most aggregations include multiple source time series, values are
 * written to arrays of longs (first attempt to preserve precision when all 
 * values are longs) or doubles (if one or more values are doubles). This allows
 * the JVM to use SIMD processing if possible. 
 * 
 * @since 3.0
 */
public class GroupByNumericPTS implements GBTypedPTS {
  
  GBNumericTs ts;
    
  /** The aggregator. */
  private NumericAggregator aggregator;
  
  /** The next timestamp to return. */
  private final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaluated when returning the next value. */
  private final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericValue dp;
    
  /** An index in the sources array used when pulling numeric iterators from the
   * sources. Must be less than or equal to the number of sources. */
  private int iterator_max;
  
  /** Used as an index into the value arrays at any given iteration. */
  private int value_idx;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  Poolable poolable;
  
  long[] long_vals;
  double[] double_vals;
  List<MutableNumericValue> values; // FFS This is bad!
  
  /**
   * Default ctor.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param sources The non-null and non-empty map of sources.
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public GroupByNumericPTS() {
    dp = new MutableNumericValue();//(MutableNumericValue) node.pipelineContext().tsdb().getRegistry().getObjectPool("MutableNumericValuePool").claim().object();
    values = Lists.newArrayList();
//    next_ts.setMax();
//    dp.resetNull(next_ts);
//    NumericAggregatorFactory agg_factory = node.pipelineContext().tsdb()
//        .getRegistry().getPlugin(NumericAggregatorFactory.class, 
//            ((GroupByConfig) node.config()).getAggregator());
//    if (agg_factory == null) {
//      throw new IllegalArgumentException("No aggregator found for type: " 
//          + ((GroupByConfig) node.config()).getAggregator());
//    }
//    aggregator = agg_factory.newAggregator(
//        ((GroupByConfig) node.config()).getInfectiousNan());
//    infectious_nan = ((GroupByConfig) node.config()).getInfectiousNan();
//    interpolators = new QueryInterpolator[series.size()];
//    
//    QueryInterpolatorConfig interpolator_config = ((GroupByConfig) node.config()).interpolatorConfig(NumericType.TYPE);
//    if (interpolator_config == null) {
//      throw new IllegalArgumentException("No interpolator config found for type");
//    }
//    
//    QueryInterpolatorFactory factory = node.pipelineContext().tsdb()
//        .getRegistry().getPlugin(QueryInterpolatorFactory.class, 
//            interpolator_config.getType());
//    if (factory == null) {
//      throw new IllegalArgumentException("No interpolator factory found for: " + 
//          interpolator_config.getType() == null ? "Default" : 
//            interpolator_config.getType());
//    }
//    
//    for (final PartialTimeSeries source : series) {
//      if (source == null) {
//        throw new IllegalArgumentException("Null time series are not "
//            + "allowed in the sources.");
//      }
//      interpolators[iterator_max] = (QueryInterpolator<NumericType>) 
//          factory.newInterpolator(NumericType.TYPE, source, interpolator_config);
//      if (interpolators[iterator_max].hasNext()) {
//        has_next = true;
//        if (interpolators[iterator_max].nextReal().compare(Op.LT, next_ts)) {
//          next_ts.update(interpolators[iterator_max].nextReal());
//        }
//      }
//      iterator_max++;
//    }
//    long_values = new long[sources.size()];
  }

//  @Override
//  public TimeStamp timestamp() {
//    return dp.timestamp();
//  }
//
//  @Override
//  public NumericType value() {
//    return dp.value();
//  }
//
//  @Override
//  public TypeToken<NumericType> type() {
//    return NumericType.TYPE;
//  }
//
//  @Override
//  public boolean hasNext() {
//    return has_next;
//  }
//  
//  @Override
//  public TimeSeriesValue<?> next() {
//    has_next = false;
//    next_next_ts.setMax();
//    value_idx = 0;
//    boolean longs = true;
//    boolean had_nan = false;
//    for (int i = 0; i < iterator_max; i++) {
//      final TimeSeriesValue<NumericType> v = interpolators[i].next(next_ts);
//      if (v == null || v.value() == null) {
//        // skip it
//      } else if (!v.value().isInteger() && Double.isNaN(v.value().doubleValue())) {
//        if (infectious_nan) {
//          if (longs) {
//            longs = false;
//            shiftToDouble();
//          }
//          double_values[value_idx++] = Double.NaN;
//        }
//        had_nan = true;
//      } else {
//        if (v.value().isInteger() && longs) {
//          long_values[value_idx++] = v.value().longValue();
//        } else {
//          if (longs) {
//            longs = false;
//            shiftToDouble();
//          }
//          double_values[value_idx++] = v.value().toDouble();
//        }
//      }
//      
//      if (interpolators[i].hasNext()) {
//        has_next = true;
//        if (interpolators[i].nextReal().compare(Op.LT, next_next_ts)) {
//          next_next_ts.update(interpolators[i].nextReal());
//        }
//      }
//    }
//    
//    // sum it
//    if (value_idx < 1) {
//      if (had_nan) {
//        dp.reset(next_ts, Double.NaN);
//      } else 
//      if (interpolators[0].fillPolicy().fill() == null) {
//        dp.resetNull(next_ts);
//      } else {
//        dp.reset(next_ts, interpolators[0].fillPolicy().fill());
//      }
//    } else {
//      if (longs) {
//        dp.resetTimestamp(next_ts);
//        aggregator.run(long_values, 0, value_idx, dp);
//      } else {
//        dp.resetTimestamp(next_ts);
//        aggregator.run(double_values, 0, value_idx, infectious_nan, dp);
//      }
//    }
//
//    next_ts.update(next_next_ts);
//
//    return this;
//  }
//  
  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NumericType.TYPE;
  }
  
  /**
   * Helper that moves all of the longs to the doubles array.
   */
//  private void shiftToDouble() {
//    if (double_values == null) {
//      double_values = new double[interpolators.length];
//    }
//    if (value_idx == 0) {
//      return;
//    }
//    for (int i = 0; i < value_idx; i++) {
//      double_values[i] = (double) long_values[i];
//    }
//  }

  @Override
  public void close() throws Exception {
    dp.close();
    
  }

  @Override
  public void setPoolable(Poolable poolable) {
    this.poolable = poolable;
  }

  @Override
  public long idHash() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultShard shard() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TypedTimeSeriesIterator iterator() {
    MyIt iterator = (MyIt) ts.gb.pipelineContext().tsdb().getRegistry().getObjectPool(GroupByNumericIteratorPool.TYPE).claim().object();
    iterator.setIterator(values.iterator());
    return iterator;
  }

  @Override
  public void addSeries(PartialTimeSeries series) {
    final TypedTimeSeriesIterator iterator = series.iterator();
    if (values.isEmpty()) {
      // first one!
      while (iterator.hasNext()) {
        MutableNumericValue new_value = (MutableNumericValue) ts.node().pipelineContext().tsdb().getRegistry().getObjectPool(MutableNumericValuePool.TYPE).claim().object();
        new_value.reset((TimeSeriesValue<NumericType>) iterator.next());
        values.add(new_value);
      }
    } else {
      int idx = 0;
      while (iterator.hasNext()) {
        TimeSeriesValue<NumericType> pts_value = (TimeSeriesValue<NumericType>) iterator.next();
        while (true) {
          MutableNumericValue extant = values.get(idx);
          if (extant.timestamp().compare(Op.EQ, pts_value.timestamp())) {
            // AGG!
            if (extant.value() == null && pts_value.value() != null) {
              extant.reset(pts_value);
            } else if (pts_value.value() != null) {
              if (extant.value().isInteger() && pts_value.value().isInteger()) {
                long_vals[0] = extant.value().longValue();
                long_vals[1] = pts_value.value().longValue();
                aggregator.run(long_vals, 0, 2, extant);
              } else {
                double_vals[0] = extant.value().toDouble();
                double_vals[1] = extant.value().toDouble();
                aggregator.run(double_vals, 0, 2, ((GroupByConfig) ts.gb.config()).getInfectiousNan(), extant);
              }
            }
            idx++;
            break;
          } else if (extant.timestamp().compare(Op.GT, pts_value.timestamp())) {
            MutableNumericValue new_value = (MutableNumericValue) ts.node().pipelineContext().tsdb().getRegistry().getObjectPool(MutableNumericValuePool.TYPE).claim().object();
            new_value.reset((TimeSeriesValue<NumericType>) iterator.next());
            values.add(idx, new_value);
            break;
          } else if (idx >= values.size()) {
            MutableNumericValue new_value = (MutableNumericValue) ts.node().pipelineContext().tsdb().getRegistry().getObjectPool(MutableNumericValuePool.TYPE).claim().object();
            new_value.reset((TimeSeriesValue<NumericType>) iterator.next());
            values.add(new_value);
            break;
          } else {
            idx++;
          }
        }
        
      }
    }
  }

  public static class MyIt implements TypedTimeSeriesIterator {

    Iterator<MutableNumericValue> iterator;
    Poolable poolable;
    
    public void setIterator(Iterator<MutableNumericValue> iterator) {
      this.iterator = iterator;
    }
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<? extends TimeSeriesDataType> next() {
      return iterator.next();
    }

    @Override
    public void setPoolable(Poolable poolable) {
      this.poolable = poolable;
    }

    @Override
    public void close() throws Exception {
      poolable.release();
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return NumericType.TYPE;
    }
    
  }
  
  public static class GroupByNumericPTSPool implements Allocator {
    public static final String TYPE = "GroupByNumericPTS";
    private static final TypeToken<?> TYPE_TOKEN = 
        TypeToken.of(GroupByNumericPTSPool.class);
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
      return new GroupByNumericPTS();
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
  
  public static class GroupByNumericIteratorPool implements Allocator {
    public static final String TYPE = "GroupByNumericIteratorPool";
    private static final TypeToken<?> TYPE_TOKEN = 
        TypeToken.of(MyIt.class);
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
      return new MyIt();
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

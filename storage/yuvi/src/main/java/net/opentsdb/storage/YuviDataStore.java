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
package net.opentsdb.storage;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.pinterest.yuvi.chunk.Chunk;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Metric;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagMatcher;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryIteratorFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;

public class YuviDataStore implements TimeSeriesDataStore/*, QueryNodeFactory*/ {
  private static final Logger LOG = LoggerFactory.getLogger(YuviDataStore.class);
  
  private final String id;
  private final YuviFactory factory;
  
  YuviDataStore(final TimeSeriesDataStoreFactory factory,
                final TSDB tsdb,
                final String id) {
    this.id = id;
    this.factory = (YuviFactory) factory;
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context,
                           String id) {
    throw new UnsupportedOperationException("Nope");
  }
  
  @Override
  public QueryNode newNode(QueryPipelineContext context,
                           String id,
                           QueryNodeConfig config) {
    return new LocalNode(null, context, (QuerySourceConfig) config, id);
  }

  @Override
  public String id() {
    return id;
  }
  
  @Override
  public Deferred<Object> write(TimeSeriesStringId id, 
                                TimeSeriesValue<?> value,
      net.opentsdb.stats.Span span) {
    
    Chunk chunk = factory.manager().getChunk(value.timestamp().epoch());
    //System.out.println("GOT CHUNK: " + chunk);
    
    final List<String> tags = Lists.newArrayListWithCapacity(id.tags().size());
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      tags.add(entry.getKey() + "=" + entry.getValue());
    }
    final Metric metric = new Metric(id.metric(), tags);
    chunk.addPoint(metric, value.timestamp().epoch(), 
        ((NumericType) value.value()).toDouble());
//    final StringBuilder buf = new StringBuilder()
//        .append("put ")
//        .append(id.metric())
//        .append(" ")
//        .append(value.timestamp().epoch())
//        .append(" ");
//    if (((NumericType) value.value()).isInteger()) {
//      buf.append(((NumericType) value.value()).longValue());
//    } else {
//      buf.append(((NumericType) value.value()).doubleValue());
//    }
//    if (id.tags() != null) {
//      buf.append(" ");
//      int i = 0;
//      for (final Entry<String, String> entry : id.tags().entrySet()) {
//        if (i++ > 0) {
//          buf.append(" ");
//        }
//        buf.append(entry.getKey())
//           .append("=")
//           .append(entry.getValue());
//      }
//    }
//    factory.manager().addMetric(buf.toString());
//    System.out.println("WROTE: " + buf.toString());
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id,
      net.opentsdb.stats.Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> join_keys,
      net.opentsdb.stats.Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    // TODO Auto-generated method stub
    return null;
  }
  
  class LocalNode extends AbstractQueryNode implements SourceNode {
    protected final QuerySourceConfig config;
    
    Query yuviQuery;
    
    LocalNode(final QueryNodeFactory factory, 
              final QueryPipelineContext context,
              final QuerySourceConfig config,
              final String id) {
      super(factory, context, id);
      this.config = config;
      List<TagMatcher> tags = Lists.newArrayList();
      if (!Strings.isNullOrEmpty(config.getFilterId())) {
        final Filter filter;
        if (config.getQuery() instanceof TimeSeriesQuery) {
          filter = ((TimeSeriesQuery) config.getQuery()).getFilter(config.getFilterId());
        } else if (config.getQuery() instanceof SemanticQuery) {
          filter = ((SemanticQuery) config.getQuery()).getFilter(config.getFilterId());
        } else {
          throw new IllegalStateException("Doh, not supported yet!");
        }
        // TODO - the rest of em
        tags = filter.getTags().stream()
            .filter(tagValueFilter -> tagValueFilter.getType().equals("wildcard"))
            .map(tagVFilter -> TagMatcher.wildcardMatch(tagVFilter.getTagk(), tagVFilter.getFilter()))
            .collect(Collectors.toList());
      }
      yuviQuery = new Query(config.getMetric(), tags);
    }
    
    @Override
    public QueryNodeConfig config() {
      return config;
    }

    @Override
    public String id() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void fetchNext(final Span span) {
      //final TimeSeriesQuery query = (TimeSeriesQuery) config.query();
      final List<TimeSeries> results = 
          YuviDataStore.this.factory.manager().query(
              yuviQuery, 
              config.startTime().epoch(), 
              config.endTime().epoch(), 
              QueryAggregation.NONE);
      onNext(new LocalResult(results));
    }
    
    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onNext(QueryResult next) {
      sendUpstream(next);
      completeUpstream(0, 0);
    }

    @Override
    public void onError(Throwable t) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public TimeStamp sequenceEnd() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Schema schema() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  ChunkManager manager() {
    return factory.manager();
  }
  
  class LocalResult implements QueryResult {
    final List<net.opentsdb.data.TimeSeries> series;
    
    LocalResult(final List<TimeSeries> results) {
      series = Lists.newArrayListWithCapacity(results.size());
      for (final TimeSeries ts : results) {
        series.add(new YuviSeries(ts));
      }
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<net.opentsdb.data.TimeSeries> timeSeries() {
      return series;
    }

    @Override
    public long sequenceId() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public QueryNode source() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    
    @Override
    public ChronoUnit resolution() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  class YuviSeries implements net.opentsdb.data.TimeSeries {
    final TimeSeries series;
    TimeSeriesStringId id;
    
    YuviSeries(final TimeSeries series) {
      this.series = series;
    }
    
    @Override
    public TimeSeriesId id() {
      if (id == null) {
        BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder();
        final String[] components = series.getMetric().split(" ");
        builder.setMetric(components[0]);
        for (int i = 1; i < components.length; i++) {
          final String[] tag_pair = components[i].split("=");
          builder.addTags(tag_pair[0], tag_pair[1]);
        }
        id = builder.build();
      }
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      if (type == NumericType.TYPE) {
        return Optional.of(new YuviIterator());
      }
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> l = 
          Lists.newArrayListWithCapacity(1);
      l.add(new YuviIterator());
      return l;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    class YuviIterator implements Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>, TimeSeriesValue<NumericType>, NumericType {
      final Iterator<Point> iterator = series.getPoints().iterator();
      Point point = null;
      TimeStamp ts = new MillisecondTimeStamp(0);
      @Override
      public boolean isInteger() {
        // always doubles
        return false;
      }

      @Override
      public long longValue() {
        throw new UnsupportedOperationException("Always doubles from Yuvi!!");
      }

      @Override
      public double doubleValue() {
        return point.getVal();
      }

      @Override
      public double toDouble() {
        return point.getVal();
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public NumericType value() {
        return this;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesValue<?> next() {
        point = iterator.next();
        ts.update(point.getTs(), 0);
        return this;
      }
      
    }
  }
  
}
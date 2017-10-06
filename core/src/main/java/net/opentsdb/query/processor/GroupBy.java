package net.opentsdb.query.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryPipeline;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;

public class GroupBy implements net.opentsdb.query.TimeSeriesProcessor, QueryListener, QueryPipeline {

  private QueryListener upstream;
  private QueryPipeline downstream;
  private final Map<String, List<String>> metric_keys;
  
  public GroupBy(QueryPipeline downstream, QueryListener sink) {
    downstream.setListener(this);
    this.downstream = downstream;
    upstream = sink;
    
    final net.opentsdb.query.pojo.TimeSeriesQuery query = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) 
        downstream.context().getQuery();
    
    metric_keys = 
        Maps.newHashMapWithExpectedSize(query.getMetrics().size());
    
    // TODO - this can be optimized by making the filters a map
    for (final Metric metric : query.getMetrics()) {
      Filter filter_set = query.getFilter(metric.getFilter());
      
      final List<String> keys = Lists.newArrayList();
      for (final TagVFilter filter : filter_set.getTags()) {
        if (filter.isGroupBy()) {
          keys.add(filter.getTagk());
        }
      }
      Collections.sort(keys);
      metric_keys.put(metric.getMetric(), keys);
    }
  }

  @Override
  public void setListener(QueryListener listener) {
    upstream = listener;
  }

  @Override
  public QueryListener getListener() {
    return upstream;
  }

  @Override
  public void fetchNext(int parallel_id) {
    downstream.fetchNext(parallel_id);
  }

  @Override
  public QueryPipeline getMultiPassClone(QueryListener listener,
      boolean cache) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addAfter(QueryPipeline pipeline) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addBefore(QueryPipeline pipeline) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() {
    downstream.close();
  }

  @Override
  public void onComplete() {
    System.out.println("GB IS complete");
    upstream.onComplete();
  }

  @Override
  public void onNext(QueryResult next) {
    upstream.onNext(new LocalResult(next));
  }

  @Override
  public void onError(Throwable t) {
    upstream.onError(t);
  }

  @Override
  public QueryPipelineContext context() {
    return downstream.context();
  }
  
  class LocalResult implements QueryResult {
    private final QueryResult next;
    private final Map<Long, TimeSeries> groups = Maps.newHashMap();
        
    public LocalResult(final QueryResult next) {
      this.next = next;
      group();
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return next.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return groups.values();
    }

    @Override
    public int parallelism() {
      return next.parallelism();
    }

    @Override
    public int parallelId() {
      return next.parallelId();
    }

    @Override
    public int sequenceId() {
      return next.sequenceId();
    }
    
    void group() {
      for (final TimeSeries series : next.timeSeries()) {
        final StringBuilder buf = new StringBuilder()
            .append(series.id().metric());
        final List<String> keys = metric_keys.get(series.id().metric());
        
        boolean matched = true;
        for (final String key : keys) {
          final String tagv = series.id().tags().get(key);
          if (tagv == null) {
            System.out.println("DROPPING: " + series.id());
            matched = false;
            break;
          }
          buf.append(tagv);
        }
        
        if (!matched) {
          continue;
        }
        
        long hash = LongHashFunction.xx_r39().hashChars(buf.toString());
        GBTimeSeries group = (GBTimeSeries) groups.get(hash);
        if (group == null) {
          group = new GBTimeSeries();
          groups.put(hash, group);
        }
        group.addSource(series);
      }
    }
    
  }
  
  class GBTimeSeries implements TimeSeries {
    List<TimeSeries> sources = Lists.newArrayList();
    Set<TypeToken<?>> types = Sets.newHashSetWithExpectedSize(1);
    TimeSeriesId id;
    MergedTimeSeriesId.Builder merging_id = MergedTimeSeriesId.newBuilder();
    
    void addSource(final TimeSeries source) {
      merging_id.addSeries(source.id());
      sources.add(source);
      types.addAll(source.types());
    }
    
    @Override
    public TimeSeriesId id() {
      if (id == null) {
        id = merging_id.build();
      }
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      if (type == NumericType.TYPE) {
        return Optional.of(new LocalIterator());
      }
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      return Lists.newArrayList(new LocalIterator());
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return types;
    }

    @Override
    public void close() {
      for (final TimeSeries ts : sources) {
        ts.close();
      }
    }
    
    class LocalIterator implements Iterator<TimeSeriesValue<?>>, TimeSeriesValue<NumericType> {
      boolean has_next = false;
      long next_ts = Long.MAX_VALUE;
      TimeStamp ts = new MillisecondTimeStamp(0);
      int iterator_max = 0;
      MutableNumericType dp = new MutableNumericType();
      Iterator<TimeSeriesValue<?>>[] iterators;
      TimeSeriesValue<NumericType>[] values;
      
      LocalIterator() {
        iterators = new Iterator[sources.size()];
        values = new TimeSeriesValue[sources.size()];
        for (int i = 0; i < sources.size(); i++) { 
          Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> op = 
              sources.get(0).iterator(NumericType.TYPE);
          if (op.isPresent()) {
            iterators[iterator_max] = op.get();
            if (iterators[iterator_max].hasNext()) {
              values[iterator_max] = (TimeSeriesValue<NumericType>) iterators[iterator_max].next();
              if (values[iterator_max].timestamp().msEpoch() < next_ts) {
                next_ts = values[iterator_max].timestamp().msEpoch();
              }
              has_next = true;
            }
            iterator_max++;
          }
        }
      }
      

      @Override
      public TimeStamp timestamp() {
        return dp.timestamp();
      }

      @Override
      public NumericType value() {
        return dp;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }

      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<?> next() {
        has_next = false;
        try {
          
          long next_next_ts = Long.MAX_VALUE;
          long sum = 0;
          for (int i = 0; i < sources.size(); i++) {
            if (values[i] == null) {
              // TODO - fill
              continue;
            }
            if (values[i].timestamp().msEpoch() == next_ts) {
              sum += values[i].value().longValue();
              if (iterators[i].hasNext()) {
                values[i] = (TimeSeriesValue<NumericType>) iterators[i].next();
                if (values[i].timestamp().msEpoch() < next_next_ts) {
                  next_next_ts = values[i].timestamp().msEpoch();
                }
                has_next = true;
              } else {
                values[i] = null;
              }
            } else {
              if (values[i].timestamp().msEpoch() > next_next_ts) {
                next_next_ts = values[i].timestamp().msEpoch();
                has_next = true;
              }
            }
          }
          
          ts.updateMsEpoch(next_ts);
          dp.reset(ts, sum);
//          if (cache) {
//            System.arraycopy(Bytes.fromLong(next_ts), 0, data, cache_idx, 8);
//            cache_idx += 8;
//            System.arraycopy(Bytes.fromLong(sum), 0, data, cache_idx, 8);
//            cache_idx += 8;
//            if (!has_next) {
//              Map<TSByteId, byte[]> c = parent.local_cache.get(parent.local_cache.size() - 1);
//              c.put(id, Arrays.copyOf(data, cache_idx));
//            }
//          }
          next_ts = next_next_ts;
  
          return this;
        } catch (Exception e){ 
          e.printStackTrace();
          throw new RuntimeException("WTF?", e);
        }
      }
      
    }
  }
}

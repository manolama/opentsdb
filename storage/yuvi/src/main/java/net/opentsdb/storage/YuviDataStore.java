package net.opentsdb.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.chunk.QueryAggregation;
import com.pinterest.yuvi.models.Point;
import com.pinterest.yuvi.models.TimeSeries;
import com.pinterest.yuvi.tagstore.Query;
import com.pinterest.yuvi.tagstore.TagMatcher;
import com.pinterest.yuvi.writer.FileMetricWriter;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.DefaultIteratorGroup;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.CachingQueryExecutor;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Bytes.ByteMap;

public class YuviDataStore extends TimeSeriesDataStore {
  private static final Logger LOG = LoggerFactory.getLogger(YuviDataStore.class);
  ChunkManager manager;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    try {
      LOG.info("Initializing Yuvi store...");
      manager = new ChunkManager("OpenTSDB", 10485760);
      Path filePath = Paths.get(tsdb.getConfig().getString("yuvi.datafile"));
      FileMetricWriter metricWriter = new FileMetricWriter(filePath, manager);
      metricWriter.start();
      LOG.info("Finished initializing Yuvi store.");
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Deferred<Object> write(TimeSeriesValue<?> value, TsdbTrace trace,
      Span upstream_span) {
    return Deferred.fromError(new UnsupportedOperationException("Boo!"));
  }

  @Override
  public QueryExecution<IteratorGroups> runTimeSeriesQuery(QueryContext context,
      TimeSeriesQuery query, Span upstream_span) {
    
    class MyExecution extends QueryExecution<IteratorGroups> {
      
      public MyExecution(TimeSeriesQuery query) {
        super(query);
        
        if (context.getTracer() != null) {
          setSpan(context, 
              YuviDataStore.this.getClass().getSimpleName(), 
              upstream_span,
              TsdbTrace.addTags(
                  "order", Integer.toString(query.getOrder()),
                  "query", JSON.serializeToString(query),
                  "startThread", Thread.currentThread().getName()));
        }
        
        //DefaultIteratorGroups groups = new DefaultIteratorGroups();
        MyGroups groups = new MyGroups();
        
        for (final Metric metric : query.getMetrics()) {
          Span metric_span = null;
          if (context.getTracer() != null) {
            metric_span = context.getTracer()
                .buildSpan("YuviQuery_" + metric.getMetric())
                .asChildOf(tracer_span)
                .start();
          }
          List<TagMatcher> tags = null;
          if (!Strings.isNullOrEmpty(metric.getFilter())) {
            for (final Filter filter : query.getFilters()) {
              if (!filter.getId().equals(metric.getFilter())) {
                continue;
              }
             
              // TODO - the rest of em
              tags = filter.getTags().stream()
                  .filter(tagValueFilter -> tagValueFilter.getType().equals("wildcard"))
                  .map(tagVFilter -> TagMatcher.wildcardMatch(tagVFilter.getTagk(), tagVFilter.getFilter()))
                  .collect(Collectors.toList());
             
              break;
            }
          }
          
          fetchMetric(context, metric, tags, query, groups, metric_span);
        }
        
        callback(groups);
      }

      @Override
      public void cancel() {
        // TODO Auto-generated method stub
        
      }
    }

    return new MyExecution(query);
  }

  @Override
  public String id() {
    return "YuviStorage";
  }

  @Override
  public String version() {
    return "1.0.0";
  }
  
  void fetchMetric(QueryContext context, final Metric metric, final List<TagMatcher> filter, 
      final TimeSeriesQuery query, final IteratorGroups groups,
      final Span metric_span) {
    
    Span query_span = null;
    if (metric_span != null) {
      query_span = context.getTracer().buildSpan("YuviQuery")
          .asChildOf(metric_span)
          .start();
    }
    final Query yuviQuery = new Query(metric.getMetric(), filter);
    final List<TimeSeries> results = manager.query(yuviQuery, 
        query.getTime().startTime().epoch(), 
        query.getTime().endTime().epoch(), 
        QueryAggregation.NONE);
    if (query_span != null) {
      query_span.setTag("status", "OK")
                .finish();
    }
    
    long dps = 0;
    final TimeSeriesGroupId id = new SimpleStringGroupId(
        Strings.isNullOrEmpty(metric.getId()) ? metric.getMetric() : metric.getId());
    MyGroup group = new MyGroup(id, results);
    groups.addGroup(group);
//    for (final TimeSeries result : results) {
//      if (result.getPoints() == null || result.getPoints().isEmpty()) {
//        continue;
//      }
//      //groups.addIterator(group, new SeriesConverter(result));
//      dps += result.getPoints().size();
//    }
    
    if (metric_span != null) {
     metric_span.setTag("timeSeries", results.size())
                .setTag("totalDataPoints", dps)
                .setTag("status", "ok")
                .finish();
    }
  }
  
  class MyGroup extends IteratorGroup {
    
    List<TimeSeries> yuvi_data;
    
    public MyGroup(TimeSeriesGroupId group, List<TimeSeries> yuvi_data) {
      super(group);
      this.yuvi_data = yuvi_data;
    }

    @Override
    public Iterator<TimeSeriesIterators> iterator() {
      return new MyIterator();
    }

    @Override
    public Deferred<Object> initialize() {
      return Deferred.fromResult(null);
    }

    @Override
    public void addIterator(TimeSeriesIterator<?> iterator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addIterators(TimeSeriesIterators set) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesIterators> iterators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesIterator<?>> iterators(TypeToken<?> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesIterator<?>> flattenedIterators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setContext(QueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Deferred<Object> close() {
      return Deferred.fromResult(null);
    }

    @Override
    public IteratorGroup getCopy(QueryContext context) {
      throw new UnsupportedOperationException();
    }
    
    class MyIterator implements Iterator<TimeSeriesIterators> {
      Iterator<TimeSeries> iterator = yuvi_data.iterator();
      
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesIterators next() {
        final TimeSeries ts = iterator.next();
        return new MyIterators(ts, new IDConverter(ts));
      }
    }
    
  }
  
  class MyIterators extends TimeSeriesIterators {
    final TimeSeries ts;
    
    public MyIterators(TimeSeries ts, TimeSeriesId id) {
      super(id);
      this.ts = ts;
    }

    @Override
    public Iterator<TimeSeriesIterator<?>> iterator() {
      return new MyIterator();
    }

    @Override
    public Deferred<Object> initialize() {
      return Deferred.fromResult(null);
    }

    @Override
    public void addIterator(TimeSeriesIterator<?> iterator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<TimeSeriesIterator<?>> iterators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeSeriesIterator<?> iterator(TypeToken<?> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setContext(QueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Deferred<Object> close() {
      return Deferred.fromResult(null);
    }

    @Override
    public TimeSeriesIterators getCopy(QueryContext context) {
      throw new UnsupportedOperationException();
    }
    
    class MyIterator implements Iterator<TimeSeriesIterator<?>> {
      boolean called = false;
      @Override
      public boolean hasNext() {
        if (called) {
          return false;
        }
        called = true;
        return true;
      }

      @Override
      public TimeSeriesIterator<?> next() {
        return new SeriesConverter(ts);
      }
      
    }
    
  }
  
  class MyGroups extends IteratorGroups {
    Map<TimeSeriesGroupId, IteratorGroup> groups = Maps.newHashMap();
    
    @Override
    public Deferred<Object> initialize() {
      return Deferred.fromResult(null);
    }

    @Override
    public void addIterator(TimeSeriesGroupId id,
                            TimeSeriesIterator<?> iterator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void addGroup(IteratorGroup group) {
     groups.put(group.id(), group);
    }

    @Override
    public IteratorGroup group(TimeSeriesGroupId id) {
      return groups.get(id);
    }

    @Override
    public Iterator<Entry<TimeSeriesGroupId, IteratorGroup>> iterator() {
      return groups.entrySet().iterator();
    }

    @Override
    public List<TimeSeriesIterator<?>> flattenedIterators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<IteratorGroup> groups() {
      return Lists.newArrayList(groups.values());
    }

    @Override
    public void setContext(QueryContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Deferred<Object> close() {
      return Deferred.fromResult(null);
    }

    @Override
    public IteratorGroups getCopy(QueryContext context) {
      throw new UnsupportedOperationException();
    }
    
  }
  
  class IDConverter extends TimeSeriesId {
    String[] components;
    
    public IDConverter(final TimeSeries source) {
      components = source.getMetric().split(" ");
    }
    
    @Override
    public List<byte[]> metrics() {
      if (metrics == null) {
        metrics = Lists.newArrayListWithCapacity(1);
        metrics.add(components[0].getBytes(Const.UTF8_CHARSET));
      }
      return metrics;
    }
    
    @Override
    public ByteMap<byte[]> tags() {
      if (tags.isEmpty() && components.length > 1) {
        tags = new ByteMap<byte[]>();
        for (int i = 1; i < components.length; i++) {
          String[] tag = components[i].split("=");
          tags.put(tag[0].getBytes(Const.UTF8_CHARSET), tag[1].getBytes(Const.UTF8_CHARSET));
        }
      }
      return tags;
    }
  }
  
  class SeriesConverter extends TimeSeriesIterator<NumericType> {
    final TimeSeries source;
    Iterator<Point> iterator;
    MutableNumericType dp;
    TimeStamp ts;
    
    public SeriesConverter(final TimeSeries source) {
      super(new IDConverter(source));
      this.source = source;
      iterator = source.getPoints().iterator();
      dp = new MutableNumericType(id);
      ts = new MillisecondTimeStamp(0);
    }
    
    @Override
    public TypeToken<? extends TimeSeriesDataType> type() {
      return NumericType.TYPE;
    }

    @Override
    public TimeStamp startTime() {
      if (source.getPoints() == null || source.getPoints().isEmpty()) {
        return new MillisecondTimeStamp(0);
      }
      return new MillisecondTimeStamp(source.getPoints().get(0).getTs() * 1000);
    }

    @Override
    public TimeStamp endTime() {
      if (source.getPoints() == null || source.getPoints().isEmpty()) {
        return new MillisecondTimeStamp(0);
      }
      return new MillisecondTimeStamp(source.getPoints().get(source.getPoints().size() - 1).getTs() * 1000);
    }

    @Override
    public IteratorStatus status() {
      return iterator.hasNext() ? IteratorStatus.HAS_DATA : IteratorStatus.END_OF_DATA;
    }

    @Override
    public TimeSeriesValue<NumericType> next() {
      Point p = iterator.next();
      ts.updateEpoch(p.getTs());
      dp.reset(ts, p.getVal(), 1);
      return dp;
    }

    @Override
    public TimeSeriesValue<NumericType> peek() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeSeriesIterator<NumericType> getShallowCopy(
        QueryContext context) {
      return new SeriesConverter(source);
    }

    @Override
    public TimeSeriesIterator<NumericType> getDeepCopy(QueryContext context,
        TimeStamp start, TimeStamp end) {
      return new SeriesConverter(source);
    }

    @Override
    protected void updateContext() {
      // TODO
    }
    
  }
  
}

package net.opentsdb.query.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Metric;

public class JexlExpression implements net.opentsdb.query.TimeSeriesProcessor, QueryListener, QueryPipeline {
  private QueryListener upstream;
  private QueryPipeline downstream;
  private Map<String, String> metric_to_ids; 
 
  Map<Integer, QueryResult[]> results;
  
  public JexlExpression(QueryPipeline downstream, QueryListener sink) {
    downstream.setListener(this);
    this.downstream = downstream;
    upstream = sink;
    
    results = Maps.newConcurrentMap();
    final net.opentsdb.query.pojo.TimeSeriesQuery query = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) 
        downstream.context().getQuery();
    metric_to_ids = Maps.newHashMap();
    
    for (final Metric metric : query.getMetrics()) {
      metric_to_ids.put(metric.getMetric(), metric.getId());
    }
    System.out.println("METRICS: " + metric_to_ids);
  }
  
  @Override
  public QueryPipelineContext context() {
    return downstream.context();
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
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete() {
    upstream.onComplete();
  }

  @Override
  public void onNext(QueryResult next) {
    System.out.println("JEXL got next");
    // TODO - it's possible to optimize based on the results in that if there are
    // a number of expressions with different metrics, e.g. e1 = a + b, e2 = c + d
    // then we could fire off e1 as soon as we get a and b.
    // for now... accumulate within chunks
    try {
    QueryResult[] result = results.get(next.sequenceId());
    if (result == null) {
      result = new QueryResult[downstream.context().parallelQueries()];
      QueryResult[] raced_result = results.putIfAbsent(next.sequenceId(), result);
      if (raced_result != null) {
        result = raced_result;
      }
    }
    
    boolean all_there = true;
    synchronized(result) {
      result[next.parallelId()] = next;
      for (QueryResult r : result) {
        if (r == null) {
          all_there = false;
          break;
        }
      }
    }
    
    if (all_there) {
      System.out.println("Got all the results...");
      // work on the segment, all our raw data is in.
      upstream.onNext(new LocalResult(result));
    } else {
      System.out.println("Still waiting for data...");
    }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onError(Throwable t) {
    upstream.onError(t);
  }

  enum VertexType {
    METRIC,
    EXPRESSION
  }
  
  class ExpVertex {
    Map<String, List<TimeSeries>> sources;
    boolean joined;
    Map<Long, TimeSeries> iterators;
    String expression;
    
    final String id;
    final VertexType type;
    
    ExpVertex(String id, final VertexType type) {
      this.id = id;
      this.type = type;
    }
    
    public void addSource(final String id, final TimeSeries source) {
      if (sources == null) {
        sources = Maps.newHashMap();
      }
      List<TimeSeries> set = sources.get(id);
      if (set == null) {
        set = Lists.newArrayList();
        sources.put(id, set);
      }
      set.add(source);
    }
    
    @Override
    public int hashCode() {
      return id.hashCode();
    }
    
    @Override
    public boolean equals(final Object obj) {
      return ((ExpVertex) obj).id.equals(id);
    }
    
    public Collection<TimeSeries> join() {
      if (!joined) {
        iterators = Maps.newHashMap();
        for (Entry<String, List<TimeSeries>> entry : sources.entrySet()) {
          for (final TimeSeries source : entry.getValue()) {
            try {
              long key = joinKey(source.id());
              //System.out.println("KEY: " + key + " " + source.id());
              TimeSeries ts = iterators.get(key);
              if (ts == null) {
                ts = new ExpTS();
                ((ExpTS) ts).metric = id;
                ((ExpTS) ts).expression_id = id;
                ((ExpTS) ts).exp = expression;
                iterators.put(key, ts);
              }
              ((ExpTS) ts).sources.put(entry.getKey(), source);
              
            } catch (IllegalArgumentException e) {
              // TODO - log it
              System.out.println(e.getMessage());
            }
          }
        }
        joined = true;
      }
      return iterators.values();
    }
    
    long joinKey(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null");
      }
      final StringBuffer buffer = new StringBuffer();
      
      final List<String> tag_keys = Lists.newArrayList("host","dc");//config.getTagKeys();
      final boolean include_agg_tags = false;//config.getExpression().getJoin() != null ?
          //config.getExpression().getJoin().getIncludeAggTags() : false;
      final boolean include_disjoint_tags = false;//config.getExpression().getJoin() != null ?
          //config.getExpression().getJoin().getIncludeDisjointTags() : false;
      if (tag_keys != null) {
        for (final String tag_key : tag_keys) {
          String tag_value = id.tags().get(tag_key);
          if (tag_value != null) {
            buffer.append(tag_key);
            buffer.append(tag_value);
          } else {
            boolean matched = false;
            if (include_agg_tags) {
              for (final String tag : id.aggregatedTags()) {
                if (tag_key.equals(tag)) {
                  matched = true;
                  break;
                }
              }
            } if (!matched && include_disjoint_tags) {
              for (final String tag : id.disjointTags()) {
                if (tag_key.equals(tag)) {
                  matched = true;
                  break;
                }
              }
            }
            if (!matched) {
//              if (LOG.isDebugEnabled()) {
//                LOG.debug("Ignoring series " + id + " as it doesn't have the "
//                    + "required tags ");
//              }
              throw new IllegalArgumentException("Failed to match the series, drop it.");
            }
            buffer.append(tag_key);
          }
        }
      } else {
        // full join!
        if (!id.tags().isEmpty()) {
          // make sure it's sorted is already sorted
          final Map<String, String> tags;
          if (id instanceof TreeMap) {
            tags = id.tags();
          } else {
            tags = new TreeMap<String, String>(id.tags());
          }
          for (final Entry<String, String> pair : tags.entrySet()) {
            buffer.append(pair.getKey());
            buffer.append(pair.getValue());
          }
        }
        
        if (include_agg_tags && !id.aggregatedTags().isEmpty()) {
          // not guaranteed of sorting
          final List<String> sorted = Lists.newArrayList(id.aggregatedTags());
          Collections.sort(sorted);
          for (final String tag : sorted) {
            buffer.append(tag);
          }
        }
        
        if (include_disjoint_tags && !id.disjointTags().isEmpty()) {
          // not guaranteed of sorting
          final List<String> sorted = Lists.newArrayList(id.disjointTags());
          Collections.sort(sorted);
          for (final String tag : sorted) {
            buffer.append(tag);
          }
        }
      }
      
      return LongHashFunction.xx_r39().hashChars(buffer.toString());
    }
  }
  
  class LocalResult implements QueryResult {
    QueryResult[] results;
    final DirectedAcyclicGraph<ExpVertex, DefaultEdge> graph;
    final List<TimeSeries> series;
    
    LocalResult(QueryResult[] results) {
      series = Lists.newArrayList();
      this.results = results;
      graph = new DirectedAcyclicGraph<ExpVertex, DefaultEdge>(DefaultEdge.class);
      
      final net.opentsdb.query.pojo.TimeSeriesQuery query = 
          (net.opentsdb.query.pojo.TimeSeriesQuery) 
          downstream.context().getQuery();
      Map<String, ExpVertex> metrics = Maps.newHashMap();

      for (final String metric : metric_to_ids.values()) {
        ExpVertex m = new ExpVertex(metric, VertexType.METRIC);
        graph.addVertex(m);
        metrics.put(metric, m);
      }
      
      // build graph of expressions
      // TODO - optimize out anything not "outputed"
      for (final Expression e : query.getExpressions()) {
        ExpVertex exp = new ExpVertex(e.getId(), VertexType.EXPRESSION);
        if (!graph.containsVertex(exp)) {
          graph.addVertex(exp);
          exp.expression = e.getExpr();
        } else {
          for (final ExpVertex extant : graph.vertexSet()) {
            if (extant.equals(exp)) {
              exp = extant;
              break;
            }
          }
        }
        for (final String var : e.getVariables()) {
          ExpVertex v = metrics.get(var);
          if (v == null) {
            v = new ExpVertex(var, VertexType.EXPRESSION);
            System.out.println("EXP: " + e.getExpr());
            v.expression = e.getExpr();
          }
          graph.addEdge(exp, v);
        }
      }
      
      for (QueryResult r : results) {
        for (TimeSeries series : r.timeSeries()) {
          ExpVertex metric = metrics.get(metric_to_ids.get(series.id().metric()));
          metric.addSource(metric.id, series);
          //System.out.println("Adding metric ID: " + series.id().metric() + " to vert: " + metric.id + " @" + System.identityHashCode(metric));
        }
      }
      
      // now build roots and link into nested expressions
      final DepthFirstIterator<ExpVertex, DefaultEdge> df_iterator = 
          new DepthFirstIterator<ExpVertex, DefaultEdge>(graph);
      while (df_iterator.hasNext()) {
        final ExpVertex vertex = df_iterator.next();
        System.out.println("Working vertex: " + vertex.id);
        
        if (vertex.type == VertexType.METRIC) {
          continue;
        }
        
        final Set<DefaultEdge> oeo = graph.outgoingEdgesOf(vertex);
        for (DefaultEdge e : oeo) {
          ExpVertex target = graph.getEdgeTarget(e);
          System.out.println("  target: " + target.id);
          if (target.type == VertexType.EXPRESSION) {
            target.join();
            for (TimeSeries source : target.iterators.values()) {
              vertex.addSource(target.id, source);
            }
          } else {
            if (target.sources != null) {
              for (List<TimeSeries> sources : target.sources.values()) {
                for (TimeSeries source : sources) {
                  //System.out.println("Adding source to " + vertex.id + ": " + source.id());
                  vertex.addSource(target.id, source);
                }
              }
            } else {
              //System.out.println("Sources were null!!! for " + target.id + " @" + System.identityHashCode(target));
            }
          }
        }
        
        if (graph.incomingEdgesOf(vertex).isEmpty()) {
          System.out.println("JOINING on " + vertex.id);
          vertex.join();
          series.addAll(vertex.iterators.values());
        }
      }
      
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return series;
    }

    @Override
    public int parallelism() {
      return 0;
    }

    @Override
    public int parallelId() {
      return 0;
    }

    @Override
    public int sequenceId() {
      return results[0].sequenceId();
    }

  }
  
  class ExpTS implements TimeSeries {

    Map<String, TimeSeries> sources = Maps.newHashMap();
    String metric;
    String expression_id;
    TimeSeriesId id;
    String exp;
    
    @Override
    public TimeSeriesId id() {
      if (id == null) {
        MergedTimeSeriesId.Builder builder = MergedTimeSeriesId.newBuilder()
            .setMetric(metric)
            .setAlias(expression_id);
        for (final TimeSeries source : sources.values()) {
          builder.addSeries(source.id());
        }
        id = builder.build();
      }
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      if (type == NumericType.TYPE) {
        return Optional.of(new LocalIterator(exp));
      }
      return Optional.empty();
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return Lists.newArrayList(NumericType.TYPE);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
    class LocalIterator implements  Iterator<TimeSeriesValue<?>>, TimeSeriesValue<NumericType> {
      boolean has_next = false;
      long next_ts = Long.MAX_VALUE;
      TimeStamp ts = new MillisecondTimeStamp(0);
      int iterator_max = 0;
      MutableNumericType dp = new MutableNumericType();
      Map<String, Iterator<TimeSeriesValue<?>>> iterators;
      Map<String, TimeSeriesValue<NumericType>> values;
      private final Script script;
      // TODO - see if this can be shared
      private final JexlContext jexl_context = new MapContext();
      
      LocalIterator(String exp) {
        script = Expression.JEXL_ENGINE.createScript(exp);
        iterators = Maps.newHashMapWithExpectedSize(sources.size());
        values = Maps.newHashMapWithExpectedSize(sources.size());
        for (Entry<String, TimeSeries> entry : sources.entrySet()) {
          Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> op = 
              entry.getValue().iterator(NumericType.TYPE);
          if (op.isPresent()) {
            Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator = op.get();
            iterators.put(entry.getKey(), iterator);
            if (iterator.hasNext()) {
              TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) iterator.next();
              values.put(entry.getKey(), value);
              if (value.timestamp().msEpoch() < next_ts) {
                next_ts = value.timestamp().msEpoch();
              }
              has_next = true;
            } else {
              values.put(entry.getKey(), null);
            }
          } else {
            iterators.put(entry.getKey(), null);
            values.put(entry.getKey(), null);
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
          for (final String var : sources.keySet()) {
            TimeSeriesValue<NumericType> v = values.get(var);
            if (v == null) {
              // TODO - fill
              jexl_context.set(var, 0L);
            } else if (v.timestamp().msEpoch() == next_ts) {
              jexl_context.set(var, v.value().isInteger() ? v.value().longValue() : v.value().doubleValue());
              
              Iterator<TimeSeriesValue<?>> it = iterators.get(var);
              if (it.hasNext()) {
                v = (TimeSeriesValue<NumericType>) it.next();
                values.put(var, v);
                if (v.timestamp().msEpoch() < next_next_ts) {
                  next_next_ts = v.timestamp().msEpoch();
                }
                has_next = true;
              } else {
                values.put(var, null);
              }
            } else {
              if (v.timestamp().msEpoch() < next_next_ts) {
                next_next_ts = v.timestamp().msEpoch();
                has_next = true;
              }
            }
          }
          
          ts.updateMsEpoch(next_ts);
          final Object output = script.execute(jexl_context);
          if (output instanceof Double) {
//            if (Double.isNaN((Double) output) && 
//                config.getExpression().getFillPolicy() != null) {
//              // TODO - infectious nan
//              dp.reset(context.syncTimestamp(), 
//                  config.getExpression().getFillPolicy().getValue());
//            } else {
//              dp.reset(context.syncTimestamp(), 
//                  (Double) output);
//            }
            dp.reset(ts, (Double) output);
          } else if (output instanceof Boolean) {
            dp.reset(ts, (((Boolean) output) ? 1 : 0));
          } else {
            throw new IllegalStateException("Expression returned a result of type: " 
                + output.getClass().getName() + " for " + this);
          }
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

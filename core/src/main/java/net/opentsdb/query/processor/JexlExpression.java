package net.opentsdb.query.processor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.JexlExpressionFactory.JEConfig;

public class JexlExpression extends AbstractQueryNode implements net.opentsdb.query.TimeSeriesProcessor {
  private JEConfig config;
  private Map<String, String> metric_to_ids;
  private Map<Integer, Map<String, QueryResult>> results;
  private int final_sequence;
  private int completed_downstream;
  
  public JexlExpression(QueryPipelineContext context, JEConfig config) {
    super(context);
    this.config = config;
    
    results = Maps.newTreeMap();
    metric_to_ids = Maps.newHashMap();
    
    for (final Metric metric : ((net.opentsdb.query.pojo.TimeSeriesQuery) context.getQuery()).getMetrics()) {
      if (config.exp.getVariables().contains(metric.getId())) {
        metric_to_ids.put(metric.getMetric(), metric.getId());
      }
    }
    System.out.println("METRICS: " + metric_to_ids);
  }
  
  @Override
  public void initialize() {
    super.initialize();
  }
  
  @Override
  public void fetchNext() {
    for (QueryNode ds : downstream) {
      ds.fetchNext();
    }
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, int final_sequence) {
    synchronized(this) {
      if (final_sequence > this.final_sequence) {
        this.final_sequence = final_sequence;
        System.out.println("   [EXP] New final seq: " + final_sequence);
      }
      completed_downstream++;
    }
  }

  @Override
  public void onNext(QueryResult next) {
    
    // TODO - it's possible to optimize based on the results in that if there are
    // a number of expressions with different metrics, e.g. e1 = a + b, e2 = c + d
    // then we could fire off e1 as soon as we get a and b.
    // for now... accumulate within chunks
    try {
      System.out.println("JEXL got next: " + next.sequenceId() + " From: " + next.source().id());
      Map<String, QueryResult> result = null;
      synchronized(this) {
        result = results.get(next.sequenceId());
        if (result == null) {
          result = Maps.newHashMap();
          results.put(next.sequenceId(), result);
        }
      }
      
      boolean all_there = true;
      synchronized(result) {
        result.put(next.source().id(), next);
        if (result.size() != downstream.size()) {
          all_there = false;
        }
        System.out.println("Results size: " + result.size() + " Expecting: " + downstream.size());
      //}
      
      if (all_there) {
        System.out.println("[EXP] Got all the results...");
        for (final QueryResult r : result.values()) {
          System.out.println(" seq: " + r.sequenceId());
        }
        // work on the segment, all our raw data is in.
        LocalResult data = new LocalResult(result.values());
        System.out.println("Finished setting up [EXP] result");
        for (final QueryNode us : upstream) {
          us.onNext(data);
        }
        System.out.println("Sent upstream from EXPRESSION...");
        
        synchronized(this) {
          boolean allin = true;
          int seq = -1;
          int last_seq = -1;
          for (Entry<Integer, Map<String, QueryResult>> entry : results.entrySet()) {
            if (!(entry.getKey() == ++seq)) {
              System.out.println("   [ALLIN?] Seq missmatch: " + entry.getKey() + " != " + seq);
              allin = false;
              break;
            }
            if (entry.getValue().size() != downstream.size()) {
              System.out.println("   [ALLIN?] Results missmatch: " + entry.getValue().size() + " != " + downstream.size());
              allin = false;
              break;
            }
            last_seq = entry.getKey();
          }
          
          if (allin) {
            System.out.println("   [ALLIN?] ALL IN!!!!!!!!!!!!!!!!");
          }
          
          if (last_seq != final_sequence) {
            System.out.println("   [ALLIN?] Sequence missmatch... *sniff*");
          }
          
          if (completed_downstream >= downstream.size() && last_seq == final_sequence && allin) {
            System.out.println("   All done from Jexl!!");
            for (final QueryNode us : upstream) {
              us.onComplete(this, next.sequenceId());
            }
          } else {
            System.out.println("  NOT done... completed ds: " + completed_downstream + "  last Seq: " + last_seq + "  Final: " + final_sequence);
          }
        }
      } else {
        System.out.println("Still waiting for data...");
      }
      
      System.out.println("---------------------");
      }
      
//      if ((context.getContext().mode() == QueryMode.SERVER_ASYNC_STREAM || 
//          context.getContext().mode() == QueryMode.SERVER_SYNC_STREAM) && complete.get()) {
//        for (final QueryNode us : upstream) {
//          us.onComplete(this, );
//        }
//      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onError(Throwable t) {
    for (final QueryNode us : upstream) {
      us.onError(t);
    }
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
    final Map<Long, TimeSeries> series; 
    final int sequence_id;
    
    LocalResult(Collection<QueryResult> results) {
      series = Maps.newHashMap();
      sequence_id = results.iterator().next().sequenceId();
      System.out.println("    joining....");
      try {
      for (final QueryResult result : results) {
        for (final TimeSeries ts : result.timeSeries()) {
          String var = config.ids_map.get(ts.id().metric());
          if (var == null) {
            System.out.println("DROPPING: " + ts.id());
            continue;
          }
          
          long key = joinKey(ts.id());
          TimeSeries exp = series.get(key);
          if (exp == null) {
            exp = new ExpTS();
            series.put(key, exp);
          }
          ((ExpTS) exp).sources.put(var, ts);
        }
      }
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("   DONE with the join");
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return series.values();
    }
    
    @Override
    public int sequenceId() {
      return sequence_id;
    }

    @Override
    public QueryNode source() {
      return JexlExpression.this;
    }
    
    long joinKey(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null");
      }
      final StringBuffer buffer = new StringBuffer();
      
      
      final List<String> tag_keys = config.exp.getJoin().getTags();
      final boolean include_agg_tags = config.exp.getJoin() != null ?
          config.exp.getJoin().getIncludeAggTags() : false;
      final boolean include_disjoint_tags = config.exp.getJoin() != null ?
          config.exp.getJoin().getIncludeDisjointTags() : false;
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
        script = Expression.JEXL_ENGINE.createScript(config.exp.getExpr());
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

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }
}

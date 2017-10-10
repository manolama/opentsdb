package net.opentsdb.query;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.execution.QueryExecutor2;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.GroupBy;
import net.opentsdb.storage.MockDataStore;
import net.opentsdb.storage.MockDataStore.MDSConfig;

public class ExecutionBuilder {

  private QueryListener listener;
  private net.opentsdb.query.pojo.TimeSeriesQuery query;
  private QueryMode mode;
  private MockDataStore executor;
  
  public ExecutionBuilder setQueryListener(final QueryListener listener) {
    this.listener = listener;
    return this;
  }
  
  public ExecutionBuilder setQuery(final net.opentsdb.query.pojo.TimeSeriesQuery query) {
    this.query = query;
    return this;
  }
  
  public ExecutionBuilder setMode(final QueryMode mode) {
    this.mode = mode;
    return this;
  }
  
  public ExecutionBuilder setExecutor(final MockDataStore executor) {
    this.executor = executor;
    return this;
  }
  
  public QueryContext build() {
    return new LocalContext(listener, query, mode, executor);
  }
  
  class LocalContext implements QueryContext {
    private final QueryListener sink;
    private net.opentsdb.query.pojo.TimeSeriesQuery query;
    private QueryMode mode;
    private QueryPipelineContext ctx;
    //private QueryNode downstream;
    private QueryNode[] roots;
    private int root_idx = 0;
    
    LocalContext(final QueryListener sink, net.opentsdb.query.pojo.TimeSeriesQuery query, QueryMode mode, MockDataStore executor) {
      this.sink = sink;
      this.query = query;
      this.mode = mode;
      ctx = new PerMetricQueryPipelineContext(query, this, executor, Lists.newArrayList(sink));
      //downstream = executor.executeQuery(ctx);
      
      ctx.initialize();
      roots = new QueryNode[ctx.roots().size()];
      int i = 0;
      for (final QueryNode root : ctx.roots()) {
        roots[i++] = root;
      }
    }
    
    @Override
    public QueryListener getListener() {
      return sink;
    }

    @Override
    public QueryMode mode() {
      return mode;
    }

    @Override
    public void fetchNext() {
      if (root_idx >= roots.length) {
        root_idx = 0;
      }
      roots[root_idx++].fetchNext();
//      for (final QueryNode root : ctx.roots()) {
//        root.fetchNext();
//      }
    }

    @Override
    public void close() {
      ctx.close();
      //downstream.close();
    }
    
//    class SingleAccumulator implements QueryNode, QueryListener, QueryResult {
//      List<TimeSeries> series;
//      boolean[] results = new boolean[ctx.parallelQueries()];
//      QueryListener listener;
//      QueryNode downstream;
//      
//      SingleAccumulator(QueryNode downstream) {
//        listener = sink;
//        this.downstream = downstream;
//        this.downstream.setListener(this);
//      }
//      @Override
//      public void onComplete() {
//        listener.onComplete();
//      }
//
//      @Override
//      public void onNext(QueryResult next) {
//        boolean complete = true;
//        synchronized (this) {
//          if (series == null) {
//            series = Lists.newArrayListWithCapacity(next.timeSeries().size());
//          }
//          series.addAll(next.timeSeries());
//          results[next.parallelId()] = true;
//          
//          for (final boolean result : results) {
//            if (!result) {
//              complete = false;
//              break;
//            }
//          }
//        }
//        
//        if (complete) {
//          System.out.println("ACUM Sending upstream");
//          listener.onNext(this);
//          System.out.println("ACUME marking complete");
//          listener.onComplete();
//        }
//      }
//
//      @Override
//      public void onError(Throwable t) {
//        listener.onError(t);
//      }
//
//      @Override
//      public TimeSpecification timeSpecification() {
//        // TODO Auto-generated method stub
//        return null;
//      }
//
//      @Override
//      public Collection<TimeSeries> timeSeries() {
//        return series;
//      }
//
//      @Override
//      public int parallelId() {
//        return 0;
//      }
//
//      @Override
//      public int sequenceId() {
//        return 0;
//      }
//
//      @Override
//      public int parallelism() {
//        return query.subQueries().size();
//      }
//
//      @Override
//      public QueryPipelineContext context() {
//        return ctx;
//      }
//
//      @Override
//      public void fetchNext(int parallel_id) {
//        downstream.fetchNext(parallel_id);
//      }
//
//      @Override
//      public void close() {
//        downstream.close();
//      }
//      @Override
//      public String id() {
//        // TODO Auto-generated method stub
//        return null;
//      }
//      
//    }
  }
}

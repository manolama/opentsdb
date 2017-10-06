package net.opentsdb.query;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.execution.QueryExecutor2;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.processor.GroupBy;
import net.opentsdb.query.processor.JexlExpression;

public class ExecutionBuilder {

  private QueryListener listener;
  private net.opentsdb.query.pojo.TimeSeriesQuery query;
  private QueryMode mode;
  private QueryExecutor2 executor;
  
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
  
  public ExecutionBuilder setExecutor(final QueryExecutor2 executor) {
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
    private QueryPipeline downstream;
    
    LocalContext(final QueryListener sink, net.opentsdb.query.pojo.TimeSeriesQuery query, QueryMode mode, QueryExecutor2 executor) {
      this.sink = sink;
      this.query = query;
      this.mode = mode;
      ctx = new PerMetricQueryPipelineContext(query, this, sink);
      downstream = executor.executeQuery(ctx);
      
      boolean gby = false;
      for (Filter f : query.getFilters()) {
        for (TagVFilter v : f.getTags()) {
          if (v.isGroupBy()) {
            gby = true;
            break;
          }
        }
      }
      
      if (query.getExpressions() != null && query.getExpressions().size() > 0) {
        System.out.println("ADDING EXP");
        downstream = new JexlExpression(downstream, sink);
      }
//      
//      if (gby) {
//        System.out.println("ADDING GROUP BY");
//        downstream = new GroupBy(downstream, sink);
//      }
      
//      if (mode == QueryMode.SINGLE && ctx.parallelQueries() > 0) {
//        downstream = new SingleAccumulator(downstream);
//      }
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
      if (mode == QueryMode.SINGLE && ctx.parallelQueries() > 0) {
        for (int i = 0; i < ctx.parallelQueries(); i++) {
          downstream.fetchNext(i);
        }
      } else {
        downstream.fetchNext(ctx.nextParallelId());
      }
    }

    @Override
    public void close() {
      ctx.close();
      downstream.close();
    }
    
    class SingleAccumulator implements QueryPipeline, QueryListener, QueryResult {
      List<TimeSeries> series;
      boolean[] results = new boolean[ctx.parallelQueries()];
      QueryListener listener;
      QueryPipeline downstream;
      
      SingleAccumulator(QueryPipeline downstream) {
        listener = sink;
        this.downstream = downstream;
        this.downstream.setListener(this);
      }
      @Override
      public void onComplete() {
        listener.onComplete();
      }

      @Override
      public void onNext(QueryResult next) {
        boolean complete = true;
        synchronized (this) {
          if (series == null) {
            series = Lists.newArrayListWithCapacity(next.timeSeries().size());
          }
          series.addAll(next.timeSeries());
          results[next.parallelId()] = true;
          
          for (final boolean result : results) {
            if (!result) {
              complete = false;
              break;
            }
          }
        }
        
        if (complete) {
          System.out.println("ACUM Sending upstream");
          listener.onNext(this);
          System.out.println("ACUME marking complete");
          listener.onComplete();
        }
      }

      @Override
      public void onError(Throwable t) {
        listener.onError(t);
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
      public int parallelId() {
        return 0;
      }

      @Override
      public int sequenceId() {
        return 0;
      }

      @Override
      public int parallelism() {
        return query.subQueries().size();
      }

      @Override
      public QueryPipelineContext context() {
        return ctx;
      }

      @Override
      public void setListener(QueryListener listener) {
        this.listener = listener;
      }

      @Override
      public QueryListener getListener() {
        return listener;
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
      
    }
  }
}

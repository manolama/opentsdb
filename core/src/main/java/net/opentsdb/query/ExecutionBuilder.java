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
    private QueryMode mode;
    private QueryPipelineContext ctx;
    //private QueryNode[] roots;
    //private int root_idx = 0;
    
    LocalContext(final QueryListener sink, net.opentsdb.query.pojo.TimeSeriesQuery query, QueryMode mode, MockDataStore executor) {
      this.sink = sink;
      this.mode = mode;
      ctx = new PerMetricQueryPipelineContext(query, this, executor, Lists.newArrayList(sink));
      ctx.initialize();
//      roots = new QueryNode[ctx.roots().size()];
//      int i = 0;
//      for (final QueryNode root : ctx.roots()) {
//        roots[i++] = root;
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
      ctx.fetchNext();
    }

    @Override
    public void close() {
      ctx.close();
      //downstream.close();
    }
    
  }
}

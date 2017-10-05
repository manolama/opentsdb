package net.opentsdb.query.context;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.AbstractQueryPipeline;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipeline;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.execution.QueryExecutorConfig;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public class QueryContext2 extends AbstractQueryPipeline implements QueryContext {

  /** The TSDB to which we belong. */
  protected final TSDB tsdb;
  
  protected final TimeSeriesQuery query;
  
  protected ExecutionGraph graph;
  
  public QueryContext2(final TSDB tsdb, final TimeSeriesQuery query) {
    this.tsdb = tsdb;
    this.query = query;
  }
  
  @Override
  public void fetchNext() {
    if (graph == null) {
      // TODO setup
      graph = tsdb.getRegistry().getDefaultExecutionGraph();
      
      // build pipeline
    } else {
      // TODO call downstream
    }
  }

  @Override
  public QueryPipeline getMultiPassClone(final QueryListener listener,
                                         final boolean cache) {
    final QueryContext2 clone = new QueryContext2(tsdb, query);
    clone.setListener(listener);
    return clone;
  }

  public TimeSeriesQuery query() {
    return query;
  }
  
  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  public QueryMode mode() {
    return QueryMode.SINGLE;
  }
  
  public QueryPipeline upstream() {
    return null;
  }

  public QueryExecutorConfig getConfigOverride(final String id) {
    return null;
  }

  @Override
  public void fetchNext(final int parallel_id) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryPipelineContext context() {
    // TODO Auto-generated method stub
    return null;
  }
}

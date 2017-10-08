package net.opentsdb.query;

import java.util.Collection;

public interface QueryPipelineContext {

  public TimeSeriesQuery getQuery(final int parallel_id);
  
  public TimeSeriesQuery getQuery();
  
  public QueryContext getContext();
  
  public void setListener(final QueryListener listener);
  
  public QueryListener getListener();
  
  public int parallelQueries();
  
  public int nextParallelId();
  
  public void initialize();
  
  public Collection<QueryListener> upstream(final QueryNode node);
  
  public Collection<QueryNode> downstream(final QueryNode node);
  
  public Collection<QueryListener> sinks();
  
  public Collection<QueryNode> roots();
  
  public void close();
  
}

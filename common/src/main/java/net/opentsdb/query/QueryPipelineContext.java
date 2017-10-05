package net.opentsdb.query;

public interface QueryPipelineContext {

  public TimeSeriesQuery getQuery(final int parallel_id);
  
  public QueryContext getContext();
  
  public void setListener(final QueryListener listener);
  
  public QueryListener getListener();
  
  public int parallelQueries();
  
  public int nextParallelId();
  
  public void close();
  
}

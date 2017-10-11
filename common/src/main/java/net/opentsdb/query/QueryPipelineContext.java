package net.opentsdb.query;

import java.util.Collection;

public interface QueryPipelineContext extends QueryNode {

  public TimeSeriesQuery getQuery(final int parallel_id);
  
  public TimeSeriesQuery getQuery();
  
  public QueryContext getContext();
  
  public void initialize();
  
  public Collection<QueryNode> upstream(final QueryNode node);
  
  public Collection<QueryNode> downstream(final QueryNode node);
  
  public Collection<QueryListener> sinks();
  
  public Collection<QueryNode> roots();
  
  public void close();
  
}

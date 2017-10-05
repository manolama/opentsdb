package net.opentsdb.query;

public abstract class AbstractQueryPipelineContext implements QueryPipelineContext {

  protected TimeSeriesQuery original_query;
  
  protected QueryContext context;
  
  protected QueryListener sink;
  
  public AbstractQueryPipelineContext(TimeSeriesQuery original_query, 
      QueryContext context, QueryListener sink) {
    this.original_query = original_query;
    this.context = context;
    this.sink = sink;
  }
  
  @Override
  public QueryContext getContext() {
    return context;
  }
  
  @Override
  public void setListener(final QueryListener listener) {
    sink = listener;
  }
  
  @Override
  public QueryListener getListener() {
    return sink;
  }
}

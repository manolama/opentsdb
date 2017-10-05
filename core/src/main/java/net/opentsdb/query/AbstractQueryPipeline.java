package net.opentsdb.query;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractQueryPipeline implements QueryPipeline {

  protected QueryListener listener;
  
  protected QueryPipeline downstream;
  
  protected AtomicBoolean closed;
  
  public AbstractQueryPipeline() {
    closed = new AtomicBoolean();
  }
  
  @Override
  public void setListener(final QueryListener listener) {
    this.listener = listener;
  }

  @Override
  public QueryListener getListener() {
    return listener;
  }
  
  @Override
  public QueryPipeline getMultiPassClone(QueryListener listener,
      boolean cache) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
  
  @Override
  public void addAfter(final QueryPipeline pipeline) {
    pipeline.addBefore(this);
  }
  
  @Override
  public void addBefore(final QueryPipeline pipeline) {
    pipeline.setListener(listener);
    downstream = pipeline;
  }
}

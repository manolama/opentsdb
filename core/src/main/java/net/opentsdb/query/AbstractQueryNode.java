package net.opentsdb.query;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractQueryNode implements QueryNode {

  protected QueryListener listener;
  
  protected QueryNode downstream;
  
  protected AtomicBoolean closed;
  
  public AbstractQueryNode() {
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
  public QueryNode getMultiPassClone(QueryListener listener,
      boolean cache) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }
 
}

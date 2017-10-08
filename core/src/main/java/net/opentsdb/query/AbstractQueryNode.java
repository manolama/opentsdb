package net.opentsdb.query;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractQueryNode implements QueryNode {

  protected QueryPipelineContext context;
  
  protected Collection<QueryListener> upstream;
  
  protected Collection<QueryNode> downstream;
  
  protected AtomicBoolean closed;
  
  public AbstractQueryNode(QueryPipelineContext context) {
    this.context = context;
    closed = new AtomicBoolean();
  }
  
  @Override
  public void initialize() {
    upstream = context.upstream(this);
    downstream = context.downstream(this);
    System.out.println(this + " us: " + upstream + "  ds: " + downstream);
  }
  
  @Override
  public QueryPipelineContext context() {
    return context;
  }
}

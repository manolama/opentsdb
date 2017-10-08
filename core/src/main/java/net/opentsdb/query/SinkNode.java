package net.opentsdb.query;

import java.util.Collection;

public class SinkNode extends AbstractQueryNode {

  Collection<QueryListener> sinks;
  
  public SinkNode(QueryPipelineContext context, Collection<QueryListener> sinks) {
    super(context);
    this.sinks = sinks;
  }
  
  @Override
  public void fetchNext(int parallel_id) {
    for (QueryNode ds : downstream) {
      ds.fetchNext(parallel_id);
    }
  }

  @Override
  public String id() {
    return "Sink";
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete() {
    for (QueryListener us : upstream) {
      us.onComplete();
    }
  }

  @Override
  public void onNext(QueryResult next) {
    for (QueryListener us : upstream) {
      us.onNext(next);
    }
  }

  @Override
  public void onError(Throwable t) {
    for (QueryListener us : upstream) {
      us.onError(t);
    }
  }
  
}

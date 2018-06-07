package net.opentsdb.query.execution.gateway;

public interface GatewayWorker {

  public void onError(Throwable t);
  
  public void onNext(GatewayQuery query);
  
  public void onComplete();
}

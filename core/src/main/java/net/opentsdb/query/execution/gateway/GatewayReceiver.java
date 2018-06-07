package net.opentsdb.query.execution.gateway;

/**
 * this describes the receivers we need to send results to. I.e. it could
 * be a message queue we need to write to, or it could be a list of a few
 * endpoints to call directly.
 */
public interface GatewayReceiver {
  
  public void send(GatewayResult result);
  
}
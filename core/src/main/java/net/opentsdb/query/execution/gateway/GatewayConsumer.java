package net.opentsdb.query.execution.gateway;

import net.opentsdb.query.QueryPipelineContext;

public interface GatewayConsumer {

  public void register(final long hash, final QueryPipelineContext context);
  
}

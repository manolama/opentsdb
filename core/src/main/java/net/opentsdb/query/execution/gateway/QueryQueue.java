package net.opentsdb.query.execution.gateway;

import java.util.List;

import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;

public interface QueryQueue {

  public Object add(long hash, List<TimeSeriesQuery> queries, final QueryPipelineContext ctx);
  
  public void poll(final long timeout, GatewayWorker worker);
}

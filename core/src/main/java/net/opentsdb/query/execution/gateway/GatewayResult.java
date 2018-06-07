package net.opentsdb.query.execution.gateway;

import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;

public interface GatewayResult {

  public TimeSeriesQuery query();
  
  public long hash();
  
  public QueryResult result();
  
  public Throwable exception();
}

package net.opentsdb.query.execution.gateway;

import java.util.List;

import net.opentsdb.auth.AuthState;
import net.opentsdb.query.TimeSeriesQuery;

public interface GatewayQuery {

  public AuthState user();
  
  public long hash();
  
  public TimeSeriesQuery query();
  
  public List<GatewayReceiver> receivers();
  
}

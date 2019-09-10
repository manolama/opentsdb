package net.opentsdb.query;

import java.util.Map;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDBPlugin;

/**
 * Filters a query and 
 */
public interface QueryFilter extends TSDBPlugin {

  public TimeSeriesQuery filter(final TimeSeriesQuery query, final AuthState auth_state, final Map<String, String> headers);
  
}

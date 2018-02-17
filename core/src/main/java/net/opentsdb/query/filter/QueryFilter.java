package net.opentsdb.query.filter;

import net.opentsdb.data.TimeSeriesQueryId;

public interface QueryFilter {

  public boolean matches(final TimeSeriesQueryId id);
  
}

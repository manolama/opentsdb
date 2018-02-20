package net.opentsdb.query.filter;

import net.opentsdb.data.TimeSeriesStringId;

public interface QueryFilter {

  public boolean matches(final TimeSeriesStringId id);
  
}

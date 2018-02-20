package net.opentsdb.query.filter;

import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;

public class TimeFilter implements QueryFilter {

  @Override
  public boolean matches(final TimeSeriesStringId id) {
    return true;
  }

  public TimeStamp startTime() {
    return null;
  }
  
  public TimeStamp endTime() {
    return null;
  }
}

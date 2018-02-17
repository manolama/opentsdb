package net.opentsdb.query.filter;

import net.opentsdb.data.TimeSeriesQueryId;
import net.opentsdb.data.TimeStamp;

public class TimeFilter implements QueryFilter {

  @Override
  public boolean matches(final TimeSeriesQueryId id) {
    return true;
  }

  public TimeStamp startTime() {
    return null;
  }
  
  public TimeStamp endTime() {
    return null;
  }
}

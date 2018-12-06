package net.opentsdb.data;

import net.opentsdb.query.QueryNode;

public interface TimeSeriesResult {

  public QueryNode node();
  
  public TimeStamp start();
  
  public TimeStamp end();
  
  public TimeSeriesId id(final long hash);
}

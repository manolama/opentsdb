package net.opentsdb.data;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.query.QueryNode;

public interface ResultShard {

  public int totalShards();
  
  public QueryNode node();
  
  public String dataSource();
  
  public TimeStamp start();
  
  public TimeStamp end();
  
  public TimeSeriesId id(final long hash);
  
  public int seriesCount();
  
  public String sourceId();
  
  // TODO - how to handle the time spec.
  
  public void close();
}

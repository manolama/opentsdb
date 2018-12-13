package net.opentsdb.data;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

public interface ResultSeries extends AutoCloseable {

  public long idHash();
  
  public ResultShard shard();
  
  public TypeToken<? extends TimeSeriesDataType> getType();
  
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator();
  
}

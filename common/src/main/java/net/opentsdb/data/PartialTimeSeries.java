package net.opentsdb.data;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

/**
 * Emitted upstream to a node from the downstream node asynchronously and from
 * multiple threads.
 *
 */
public interface PartialTimeSeries extends AutoCloseable {

  // An id for the time series ID as a primitive.
  public long idHash();
  
  // The shard this series is part of
  public ResultShard shard();
  
  // type of data in this shard
  public TypeToken<? extends TimeSeriesDataType> getType();
  
  // a thread safe iterator over the data for this series and shard slice.
  public Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator();
  
}

package net.opentsdb.storage;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

public interface StorageSeries extends Iterable<TimeSeriesValue<?>> {
    
  public void optimize();
  
  public void decode(TimeStamp base, byte[] tsuid, byte prefix, byte[] qualifier,
      byte[] value);
}

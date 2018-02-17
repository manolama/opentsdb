package net.opentsdb.storage.schemas.v1;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.storage.StorageSeries;

public interface V1Codec {
  
  public TypeToken<?> type();
  
  public StorageSeries newIterable();
}

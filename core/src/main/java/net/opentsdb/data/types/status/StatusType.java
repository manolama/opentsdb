package net.opentsdb.data.types.status;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

public interface StatusType extends TimeSeriesDataType {

  public static final TypeToken<StatusType> TYPE = TypeToken.of(StatusType.class);
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  public boolean alerting();
  
  public TimeStamp lastUpdateTime();
  
  public int statusCode();
}

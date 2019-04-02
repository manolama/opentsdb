package net.opentsdb.data;

import com.google.common.reflect.TypeToken;

public class NoDataType implements TimeSeriesDataType {

  public static final TypeToken<? extends TimeSeriesDataType> TYPE = 
      TypeToken.of(NoDataType.class);
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }

}

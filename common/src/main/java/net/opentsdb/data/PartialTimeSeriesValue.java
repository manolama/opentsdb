package net.opentsdb.data;

public interface PartialTimeSeriesValue<T extends TimeSeriesDataType> {
  
  public T value();

}

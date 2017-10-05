package net.opentsdb.data;

import java.util.Iterator;

public abstract class AbstractTimeSeries implements TimeSeries {
  protected final TimeSeriesId id;
  
  public AbstractTimeSeries(final TimeSeriesId id) {
    this.id = id;
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }
  
  public abstract class LocalIterator<T extends TimeSeriesDataType> implements 
      Iterator<TimeSeriesValue<T>> {
    protected int idx = 0;
    protected int time_spec_idx = 0;
  }
}

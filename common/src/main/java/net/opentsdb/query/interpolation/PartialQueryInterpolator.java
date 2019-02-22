package net.opentsdb.query.interpolation;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

public interface PartialQueryInterpolator<T extends TimeSeriesDataType> extends AutoCloseable {

  public void reset(final PartialQueryInterpolatorContainer<T> container, 
                    final PartialTimeSeries pts);
  
  /** @return Whether or not the underlying source has another real value. */
  public boolean hasNext();
  
  /**
   * @param timestamp A non-null timestamp to fetch a value at. 
   * @return A real or filled value. May be null. 
   */
  public TimeSeriesValue<T> next(final TimeStamp timestamp);
  
  /** @return The timestamp for the next real value if available. Returns null
   * if no real value is available. */
  public TimeStamp nextReal();
  
  
}

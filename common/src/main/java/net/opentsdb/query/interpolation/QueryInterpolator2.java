package net.opentsdb.query.interpolation;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryFillPolicy;

public interface QueryInterpolator2<T extends TimeSeriesDataType> extends AutoCloseable {

  // Call this first and use it as a trigger to reset the interpolator state
  public void setConfig(final QueryInterpolatorConfig config);
  
  public void setSeries(final PartialTimeSeries series);
  
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
  
  /** @return The fill policy used by the interpolator. */
  public QueryFillPolicy<T> fillPolicy();
  
}

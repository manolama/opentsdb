package net.opentsdb.query.interpolation;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.pools.CloseablePoolable;
import net.opentsdb.query.QueryFillPolicy;

public interface PartialQueryInterpolatorContainer<T extends TimeSeriesDataType> 
    extends CloseablePoolable {

  // Call this first and use it as a trigger to reset the interpolator state
  public void setConfig(final QueryInterpolatorConfig config);
  
  public PartialQueryInterpolator<T> newPartial(final PartialTimeSeries series);
  
  public QueryInterpolatorConfig config();
}

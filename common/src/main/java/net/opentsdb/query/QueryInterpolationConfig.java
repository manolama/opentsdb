package net.opentsdb.query;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

/**
 * The overall config determining how interpolation is handled for
 * various types
 *
 */
public interface QueryInterpolationConfig {

  public QueryInterpolatorConfig config(
      final TypeToken<? extends TimeSeriesDataType> type);
  
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(final TypeToken<? extends TimeSeriesDataType> type, final TimeSeries source, final QueryInterpolatorConfig config);
  
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(final TypeToken<? extends TimeSeriesDataType> type, final TimeSeries source);
  
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(final TypeToken<? extends TimeSeriesDataType> type, final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator, final QueryInterpolatorConfig config);
  
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(final TypeToken<? extends TimeSeriesDataType> type, final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator);
}

package net.opentsdb.query.processor.downsample;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;

public interface DownsamplePartialTimeSeries<T extends TimeSeriesDataType> 
  extends PartialTimeSeries<T> {

  
  public void addSeries(final PartialTimeSeries series);
  
  public void reset(final Downsample node, final PartialTimeSeriesSet set, final boolean multiples);
  
}

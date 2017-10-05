package net.opentsdb.data.iterators;

import java.util.Iterator;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

public abstract class AbstractIterator<T extends TimeSeriesDataType> implements 
    Iterator<TimeSeriesValue<T>> {
  
  protected int idx = 0;
  protected int time_spec_idx = 0;
}

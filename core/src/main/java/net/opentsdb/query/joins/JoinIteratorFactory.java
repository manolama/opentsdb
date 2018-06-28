package net.opentsdb.query.joins;

import java.util.Iterator;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;

public interface JoinIteratorFactory {

  Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> newIterator(
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> left,
      final Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> right,
      final JoinedTimeSeries series);
}

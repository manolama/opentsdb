package net.opentsdb.query.execution.cache;

import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

public class CombinedNumeric implements TypedTimeSeriesIterator<NumericType> {
  List<Pair<QueryResult, TimeSeries>> series;
  int idx = 0;
  TypedTimeSeriesIterator<NumericType> iterator;
  
  CombinedNumeric(final CombinedResult result, final List<Pair<QueryResult, TimeSeries>> series) {
    System.out.println(" COMBINED NUMERIC WITH: " + series.size());
    this.series = series;
    iterator = (TypedTimeSeriesIterator<NumericType>) 
        series.get(idx).getValue().iterator(NumericType.TYPE).get();
    while (idx < series.size()) {
      if (iterator.hasNext()) {
        break;
      }
      
      series.get(idx).getValue().close();
      if (++idx < series.size()) {
        iterator = (TypedTimeSeriesIterator<NumericType>) 
            series.get(idx).getValue().iterator(NumericType.TYPE).get();
      } else {
        iterator = null;
      }
    }
  }

  @Override
  public boolean hasNext() {
    while (idx < series.size()) {
      if (iterator.hasNext()) {
        return true;
      }
      series.get(idx).getValue().close();
      System.out.println("   ADV TO: " + (idx + 1));
      if (++idx < series.size()) {
        iterator = (TypedTimeSeriesIterator<NumericType>) 
            series.get(idx).getValue().iterator(NumericType.TYPE).get();
      } else {
        iterator = null;
      }
    }
    return false;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    return iterator.next();
  }

  @Override
  public TypeToken<NumericType> getType() {
    return NumericType.TYPE;
  }
}

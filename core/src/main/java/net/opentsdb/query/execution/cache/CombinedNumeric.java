// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.execution.cache;

import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

/**
 * An iterator that handles combining multiple numeric type results from the 
 * cache into a single logical result.
 * 
 * @since 3.0
 */
public class CombinedNumeric implements TypedTimeSeriesIterator<NumericType> {
  
  /** The list of source data. */
  private final List<Pair<QueryResult, TimeSeries>> series;
  
  /** The current index into the series. */
  private int idx = 0;
  
  /** The current iterator we're working on. */
  private TypedTimeSeriesIterator<NumericType> iterator;
  
  /**
   * Default ctor.
   * @param result The non-null combined result.
   * @param series The non-null result set.
   */
  CombinedNumeric(final CombinedResult result, 
                  final List<Pair<QueryResult, TimeSeries>> series) {
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

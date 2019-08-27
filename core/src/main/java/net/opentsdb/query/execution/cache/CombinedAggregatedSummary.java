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
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

/**
 * A class used for the Summarizer node in rare cases where a cached query aligns
 * exactly on the boundaries of segments. Otherwise we can't sum these up.
 * 
 * TODO - handle rollups properly.
 * 
 * @since 3.0
 */
public class CombinedAggregatedSummary implements TypedTimeSeriesIterator<NumericSummaryType> {

  /** The value we'll fill. */
  private MutableNumericSummaryValue value;
  
  /** Toggle to use when next has been called. */
  private boolean called = false;
  
  /**
   * Default ctor.
   * @param result The non-null result set.
   * @param series The non-null series.
   */
  CombinedAggregatedSummary(final CombinedResult result, 
                            final List<Pair<QueryResult, TimeSeries>> series) {
    value = new MutableNumericSummaryValue();
    
    for (int i = 0; i < series.size(); i++) {
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
          series.get(i).getValue().iterator(NumericSummaryType.TYPE);
      if (!op.isPresent()) {
        continue;
      }
      final TypedTimeSeriesIterator<NumericSummaryType> it = 
          (TypedTimeSeriesIterator<NumericSummaryType>) op.get();
      if (!it.hasNext()) {
        continue;
      }
      final TimeSeriesValue<NumericSummaryType> val = it.next();
      if (i == 0) {
        value.reset(val);
      } else {
        for (final int summary : val.value().summariesAvailable()) {
          NumericType new_dp = val.value().value(summary);
          NumericType existing = value.value(summary);
          switch (summary) {
          // TODO - don't hard code. Use the rollup configs
          case 0: // sum
          case 1: // count
            if (new_dp.isInteger() && existing.isInteger()) {
              value.resetValue(summary, new_dp.longValue() + existing.longValue());
            } else {              
              value.resetValue(summary, new_dp.toDouble() + existing.toDouble());
            }
            break;
          case 2: // min
            if (new_dp.toDouble() < existing.toDouble()) {
              value.resetValue(summary, new_dp);
            }
            break;
          case 3: // max
            if (new_dp.toDouble() > existing.toDouble()) {
              value.resetValue(summary, new_dp);
            }
            break;
          case 5:
            // avg
            // TODO - please please please say there is a better way *sniff*
            value.resetValue(summary, (new_dp.toDouble() + existing.toDouble()) / (double) 2);
            break;
          case 6:
            // first, skip since we already set it.
            break;
          case 7:
            // last
            value.resetValue(summary, new_dp);
          }
        }
      }
    }
  }
  
  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericSummaryType> next() {
    called = true;
    return value;
  }

  @Override
  public TypeToken<NumericSummaryType> getType() {
    return NumericSummaryType.TYPE;
  }

}

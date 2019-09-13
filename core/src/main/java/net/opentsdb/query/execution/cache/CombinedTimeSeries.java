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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;

/**
 * Handles returning iterators for the combined time series. Note that we assume
 * the source series are in increasing timestamp order.
 * 
 * @since 3.0
 */
public class CombinedTimeSeries implements TimeSeries {
  
  /** The result set we're a part of.*/
  protected final CombinedResult combined;
  
  /** The non-null series we're combining. */
  protected final TimeSeries[] series;
  
  /** A reference to the first non-null series. */
  protected final TimeSeries ref_ts;
  
  protected final Set<TypeToken<? extends TimeSeriesDataType>> types;
  
  /**
   * Default ctor.
   * @param combined The non-null result set we're a part.
   * @param index The index of this series in the array.
   * @param ts The non-null series.
   */
  CombinedTimeSeries(final CombinedResult combined, 
                     final int index, 
                     final TimeSeries ts) {
    this.combined = combined;
    series = new TimeSeries[combined.results().length];
    types = Sets.newHashSet();
    series[index] = ts;
    ref_ts = ts;
    types.addAll(ref_ts.types());
    if (types.size() > 1 && types.contains(NumericArrayType.TYPE)) {
      types.clear();
      types.add(NumericArrayType.TYPE);
    }
  }
  
  @Override
  public TimeSeriesId id() {
    return ref_ts.id();
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (ref_ts.types().contains(type)) {
      if (type == NumericType.TYPE) {
        return Optional.of(new CombinedNumeric(combined, series));
      } else if (type == NumericArrayType.TYPE) {
        return Optional.of(new CombinedArray(combined, series));
      } else if (type == NumericSummaryType.TYPE) {
        return Optional.of(new CombinedSummary(combined, series));
      }
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators =
        Lists.newArrayList();
    TypeToken<? extends TimeSeriesDataType> type = ref_ts.types().iterator().next();
    if (type == NumericType.TYPE) {
      iterators.add(new CombinedNumeric(combined, series));
    } else if (type == NumericArrayType.TYPE) {
      iterators.add(new CombinedArray(combined, series));
    } else if (type == NumericSummaryType.TYPE) {
      iterators.add(new CombinedSummary(combined, series));
    }
    return iterators;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return ref_ts.types();
  }

  @Override
  public void close() {
    for (int i = 0; i < series.length; i++) {
      if (series[i] == null) {
        continue;
      }
      
      series[i].close();
    }
  }

  void add(final int index, final TimeSeries ts) {
    series[index] = ts;
    types.addAll(ts.types());
    if (types.size() > 1 && types.contains(NumericArrayType.TYPE)) {
      types.clear();
      types.add(NumericArrayType.TYPE);
    }
  }
}

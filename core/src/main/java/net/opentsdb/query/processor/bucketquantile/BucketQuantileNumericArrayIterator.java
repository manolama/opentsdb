// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryIterator;

/**
 * Simple iterator that wraps up the quantiles array and returns it.
 * 
 * @since 3.0
 */
public class BucketQuantileNumericArrayIterator implements TimeSeries, 
    QueryIterator, 
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType{
  
  private final TimeStamp timestamp;
  private final double[] quantile;
  private final int quantiles_idx;
  private boolean has_next;
  private TimeSeriesId id;
  private final TimeSeriesId base_id;
  private final String as;
  private final double ptile;
  
  /**
   * Default ctor.
   * @param timestamp The timestamp to return.
   * @param quantiles The quantile array. If null, no data.
   * @param quantiles_idx The end value for the quantiles array.
   * @param base_id The base array to add to.
   * @param as The as string to use as the metric.
   * @param quantile The quantile we're measuring.
   */
  public BucketQuantileNumericArrayIterator(final TimeStamp timestamp,
                                            final double[] quantiles,
                                            final int quantiles_idx,
                                            final TimeSeriesId base_id,
                                            final String as,
                                            final double quantile) {
    this.timestamp = timestamp;
    this.quantile = quantiles;
    this.quantiles_idx = quantiles_idx;
    this.base_id = base_id;
    this.as = as;
    this.ptile = quantile;
    has_next = quantiles != null && quantiles_idx > 0 ? true : false;
  }
  
  @Override
  public TypeToken getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    has_next = false;
    return this;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return quantiles_idx;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long[] longArray() {
    return null;
  }

  @Override
  public double[] doubleArray() {
    return quantile;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericArrayType value() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeSeriesId id() {
    if (id != null) {
      return id;
    }
    // TODO - byte id
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(as)
        .setTags(Maps.newHashMap(((TimeSeriesStringId) base_id).tags()))
        // TODO - ?
        //.setAggregatedTags(((TimeSeriesStringId) base_id).aggregatedTags())
        //.setDisjointTags(((TimeSeriesStringId) base_id).disjointTags())
        .addTags(BucketQuantileFactory.PERCENTILE_TAG, Double.toString(ptile * 100))
        .build();
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericArrayType.TYPE) {
      return Optional.empty();
    }
    return Optional.of((TypedTimeSeriesIterator<? extends TimeSeriesDataType>) this);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = this;
    return Lists.<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>newArrayList(it);
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return NumericArrayType.SINGLE_LIST;
  }

}

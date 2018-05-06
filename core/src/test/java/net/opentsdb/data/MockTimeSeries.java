// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.data;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.Ignore;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;

/**
 * Simple little class for mocking out a source.
 */
@Ignore
public class MockTimeSeries<T extends TimeSeriesDataType> implements TimeSeries {
  
  public final TypeToken<?> type;
  public final TimeSeriesId id;
  public RuntimeException ex;
  public boolean throw_ex;
  public List<TimeSeriesValue<T>> data = Lists.newArrayList();
  public int closed = 0;
  
  public MockTimeSeries(final TimeSeriesId id, final TypeToken<?> type) {
    this.id = id;
    this.type = type;
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }
  
  /**
   * WARN: Doesn't check time order. Make sure you do.
   * @param value A non-null value. The value of the value can be null.
   */
  public void add(final TimeSeriesValue<T> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value can't be null!");
    }
    data.add(value);
  }
  
  /**
   * As if the iterator returned a nulled value.
   */
  public void addNull() {
    data.add(null);
  }
  
  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      final TypeToken<?> type) {
    if (type != this.type) {
      return Optional.empty();
    }
    return Optional.of(new LocalIterator());
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithCapacity(1);
    iterators.add(new LocalIterator());
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    return Lists.newArrayList(type);
  }

  @Override
  public void close() { 
    closed++;
  }
  
  class LocalIterator implements Iterator<TimeSeriesValue<?>> {
    final Iterator<TimeSeriesValue<T>> iterator = data.iterator();
    
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      if (ex != null && throw_ex) {
        throw ex;
      }
      return (TimeSeriesValue<?>) iterator.next();
    }
    
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.iterators;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;

public class SlicedTimeSeries implements TimeSeries {

  /** The list of sources to combine into a logical view. */
  private List<TimeSeries> sources;
  
  /** The types of data available. */
  private Set<TypeToken<?>> types;
  
  /**
   * Default ctor. Sets up an array and instantiates a timestamp.
   */
  public SlicedTimeSeries() {
    sources = Lists.newArrayListWithExpectedSize(1);
    types = Sets.newHashSet();
  }
  
  /**
   * Adds a source to the set in order. Must have the same ID as the previous
   * sources and a greater or equivalent start timestamp to the end timestamp
   * of the previous iterator. 
   * <b>Note:</b> The first iterator inserted sets the ID for the set.
   * @param source A non-null source to add.
   */
  public void addSource(final TimeSeries source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (sources.isEmpty()) {
      sources.add(source);
      return;
    }
    
    // validate
    // TODO - this is EXPENSIVE! 
    // We may want to add to the interface the first and last timestamp for each
    // time series.
//    final TimeSeries previous = sources.get(sources.size() - 1);
//    if (iterator.startTime().compare(TimeStampComparator.LT, previous.endTime())) {
//      throw new IllegalArgumentException("Iterator must start " 
//          + iterator.startTime() + " at or after the "
//          + "previous iterator's end time. Previous end: " + previous.endTime());
//    }
    sources.add(source);
    types.addAll(source.types());
  }
  
  @Override
  public TimeSeriesId id() {
    return sources.get(0).id();
  }
  
  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> 
    iterator(final TypeToken<?> type) {
    if (!types.contains(type)) {
      return Optional.empty();
    }
    
    return Optional.of(new LocalIterator(type));
  }
  
  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = 
        Lists.newArrayListWithCapacity(types.size());
    for (final TypeToken<?> type : types) {
      iterators.add(new LocalIterator(type));
    }
    return iterators;
  }
  
  @Override
  public Collection<TypeToken<?>> types() {
    return Collections.unmodifiableSet(types);
  }
  
  @Override
  public void close() {
    for (final TimeSeries ts : sources) {
      ts.close();
    }
  }
  
  class LocalIterator<T extends TimeSeriesDataType> implements 
    Iterator<TimeSeriesValue<T>> {
    
    private final TypeToken<?> type;
    private int source_idx;
    private Iterator<TimeSeriesValue<?>> iterator;
    
    LocalIterator(final TypeToken<?> type) {
      this.type = type;
      for (source_idx = 0; source_idx < sources.size(); source_idx++) {
        final Optional<Iterator<TimeSeriesValue<?>>> it = 
            sources.get(source_idx).iterator(type);
        if (it.isPresent()) {
          iterator = it.get();
          break;
        }
      }
    }
    
    
    @Override
    public boolean hasNext() {
      if (source_idx >= sources.size()) {
        return false;
      }
      if (iterator != null && iterator.hasNext()) {
        return true;
      }
      
      source_idx++;
      for (; source_idx < sources.size(); source_idx++) {
        final Optional<Iterator<TimeSeriesValue<?>>> it = 
            sources.get(source_idx).iterator(type);
        if (it.isPresent()) {
          iterator = it.get();
          if (iterator.hasNext()) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public TimeSeriesValue<T> next() {
      return (TimeSeriesValue<T>) iterator.next();
    }
    
  }
  
}
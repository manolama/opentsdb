// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * A simple mutable data point for holding primitive signed numbers including 
 * {@link Long}s or {@link Double}s. The class is also nullable so that if the
 * owner calls {@link #resetNull(TimeStamp)} then the calls to {@link #value()}
 * will return null but the timestamp will be accurate.
 * 
 * @since 3.0
 */
public final class MutableNumericSummaryValue implements NumericSummaryType, 
                                                 TimeSeriesValue<NumericSummaryType> {

  //NOTE: Fields are not final to make an instance available to store a new
  // pair of a timestamp and a value to reduce memory burden.
  
  /** The timestamp for this data point. */
  private TimeStamp timestamp;
  
  private Map<Integer, NumericType> values;
  
  /** Whether or not the current value is null. */
  private boolean nulled; 
  
  public MutableNumericSummaryValue() {
    timestamp = new MillisecondTimeStamp(0L);
    values = Maps.newHashMapWithExpectedSize(1);
    nulled = false;
  }
  
  public MutableNumericSummaryValue(final TimeSeriesValue<NumericSummaryType> value) {
    if (value.timestamp() == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    this.timestamp = value.timestamp().getCopy();
    values = Maps.newHashMapWithExpectedSize(value.value() != null ? value.value().summariesAvailable().size() : 1);
    if (value.value() != null) {
      for (final int summary : value.value().summariesAvailable()) {
        final MutableNumericType clone = new MutableNumericType();
        clone.set(value.value().value(summary));
        values.put(summary, clone);
      }
      nulled = false;
    } else {
      nulled = true;
    }
  }
    
  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public Collection<Integer> summariesAvailable() {
    return values.keySet();
  }
  
  @Override
  public NumericType value(final int summary) {
    return values.get(summary);
  }
  
  /**
   * Empties the hash map of all values
   */
  public void clear() {
    values.clear();
  }
  
//  /**
//   * Reset the value given the timestamp and value.
//   * @param timestamp A non-null timestamp.
//   * @param value A numeric value.
//   * @throws IllegalArgumentException if the timestamp was null.
//   */
//  public void reset(final TimeStamp timestamp, final long value) {
//    if (timestamp == null) {
//      throw new IllegalArgumentException("Timestamp cannot be null");
//    }
//    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
//      this.timestamp = timestamp.getCopy();
//    } else {
//      this.timestamp.update(timestamp);
//    }
//    this.value = value;
//    is_integer = true;
//    nulled = false;
//  }
//  
//  /**
//   * Reset the value given the timestamp and value.
//   * @param timestamp A non-null timestamp.
//   * @param value A numeric value.
//   * @throws IllegalArgumentException if the timestamp was null.
//   */
//  public void reset(final TimeStamp timestamp, final double value) {
//    if (timestamp == null) {
//      throw new IllegalArgumentException("Timestamp cannot be null");
//    }
//    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
//      this.timestamp = timestamp.getCopy();
//    } else {
//      this.timestamp.update(timestamp);
//    }
//    this.value = Double.doubleToRawLongBits(value);
//    is_integer = false;
//    nulled = false;
//  }
//  
//  /**
//   * Resets the local value by copying the timestamp and value from the source
//   * value.
//   * @param value A non-null value.
//   * @throws IllegalArgumentException if the value was null or the value's 
//   * timestamp was null.
//   */
//  public void reset(final TimeSeriesValue<NumericType> value) {
//    if (value == null) {
//      throw new IllegalArgumentException("Value cannot be null");
//    }
//    if (value.timestamp() == null) {
//      throw new IllegalArgumentException("Value's timestamp cannot be null");
//    }
//    if (value.timestamp().units().ordinal() < this.timestamp.units().ordinal()) {
//      this.timestamp = value.timestamp().getCopy();
//    } else {
//      this.timestamp.update(value.timestamp());
//    }
//    if (value.value() != null) {
//      this.value = value.value().isInteger() ? value.value().longValue() : 
//        Double.doubleToRawLongBits(value.value().doubleValue());
//      is_integer = value.value().isInteger();
//      nulled = false;
//    } else {
//      nulled = true;
//    }
//  }
//
  /**
   * Resets the local value by copying the timestamp and value from the arguments.
   * @param A non-null timestamp.
   * @param value A numeric summary value.
   * @throws IllegalArgumentException if the timestamp or value was null.
   */
  public void reset(final TimeSeriesValue<NumericSummaryType> value) {
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (value.value() == null) {
      resetNull(value.timestamp());
    } else {
      reset(value.timestamp(), value.value());
    }
  }
  
  public void reset(final TimeStamp timestamp, final NumericSummaryType value) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    final Collection<Integer> types = value.summariesAvailable();
    for (final int type : types) {
      NumericType extant = values.get(type);
      if (extant == null) {
        extant = new MutableNumericType();
        values.put(type, extant);
      }
      ((MutableNumericType) extant).set(value.value(type));
    }
    Iterator<Entry<Integer, NumericType>> iterator = values.entrySet().iterator();
    while (iterator.hasNext()) {
      final Entry<Integer, NumericType> entry = iterator.next();
      if (!types.contains(entry.getKey())) {
        iterator.remove();
      }
    }
    nulled = false;
  }
  
  /**
   * Resets the value to null with the given timestamp.
   * @param timestamp A non-null timestamp to update from.
   * @throws IllegalArgumentException if the timestamp was null.
   */
  public void resetNull(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null.");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
    clear();
    nulled = true;
  }
  
  public void resetValue(final int type, final long value) {
    NumericType v = values.get(type);
    if (v == null) {
      v = new MutableNumericType();
      values.put(type, v);
    }
    ((MutableNumericType) v).set(value);
    nulled = false;
  }
  
  public void resetValue(final int type, final double value) {
    NumericType v = values.get(type);
    if (v == null) {
      v = new MutableNumericType();
      values.put(type, v);
    }
    ((MutableNumericType) v).set(value);
    nulled = false;
  }
  
  public void resetValue(final int type, final NumericType value) {
    NumericType v = values.get(type);
    if (v == null) {
      v = new MutableNumericType();
      values.put(type, v);
    }
    ((MutableNumericType) v).set(value);
    nulled = false;
  }
  
  public void resetValue(final int type, final TimeSeriesValue<NumericType> value) {
    if (value.value() == null) {
      // todo if all are null, set null.
      return;
    } else {
      NumericType v = values.get(type);
      if (v == null) {
        v = new MutableNumericType();
        values.put(type, v);
      }
      ((MutableNumericType) v).set(value.value());
      nulled = false;
    }
  }
  
  public void nullValue(final int type) {
    values.remove(type);
    // TODO - what if all gone?
  }
  
  /**
   * Resets just the timestamp of the data point. Good for use with 
   * aggregators but be CAREFUL not to return without updating the value.
   * @param timestamp The timestamp to set.
   */
  public void resetTimestamp(final TimeStamp timestamp) {
    if (timestamp == null) {
      throw new IllegalArgumentException("Timestamp cannot be null");
    }
    if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
      this.timestamp = timestamp.getCopy();
    } else {
      this.timestamp.update(timestamp);
    }
  }
  
  @Override
  public TypeToken<NumericSummaryType> type() {
    return NumericSummaryType.TYPE;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("timestamp=")
        .append(timestamp)
        .append(", nulled=")
        .append(nulled)
        .append(", values=")
        .append(values)
        .toString();
  }

  @Override
  public NumericSummaryType value() {
    return nulled ? null : this;
  }
}

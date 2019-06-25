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
package net.opentsdb.data.types.numeric;

import java.time.temporal.ChronoUnit;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;

/**
 * An encoding of timestamp and numeric values in a {@link long[]} for better
 * cache usage so that we're working on a vector instead of iterators. This is
 * 15% more efficient than the old iterative method and 4% more efficient than 
 * having an iterator on top of this array.
 * <p>
 * The format is an array of long primitives. The first entry is a 4 bit header 
 * and full epoch timestamp in seconds or milliseconds. The second entry is
 * either a nanosecond offset if the timestamp is in nanoseconds or the value
 * as either a raw long or a long encoded double precision value.
 * The flags are:
 * 64 - 0 == raw long, 1 == double value
 * 63 + 62 - 00 == seconds, 10 == milliseconds, 01 == nanoseconds
 * 
 * TODO - probably a better way. E.g. we can do the delta of delta of timestamps
 * like Gorilla though we need the external ref and another calculation. Gotta
 * bench that for cache hits/misses, etc.
 * 
 * TODO - may default to this and re-do the data types. Though downsampled is 
 * still faster since we don't deal with timestamps.
 * 
 * @since 3.0
 */
public interface NumericLongArrayType extends TimeSeriesDataType {
  public static final TypeToken<NumericLongArrayType> TYPE = 
      TypeToken.of(NumericLongArrayType.class);
  
  /** Flag set when the timestamp is in milliseconds instead of seconds or 
   * nanoseconds. */
  public static final long MILLISECOND_FLAG = 0x4000000000000000L;
  
  /** Flag set when the timestamp is in nanoseconds in which case the next 
   * long is the nanos offset. */
  public static final long NANOSECOND_FLAG = 0x2000000000000000L;
  
  /** Flag set when the long value is an encoded double. Otherwise it's a 
   * straight long. */
  public static final long FLOAT_FLAG = 0x8000000000000000L;
  
  /** A mask to zero out the flag bits before reading the timestamp. */
  public static final long TIMESTAMP_MASK = 0xFFFFFFFFFFFFFFFL;
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  /**
   * The starting offset into the array where data begins for this value. Used
   * for shared arrays. For non-shared arrays this should always be zero.
   * @return The starting offset into the array.
   */
  public int offset();

  /**
   * The index into the array where data is no longer valid. E.g. for a 
   * while loop you'd write {code for (int i = offset(); i < end(); i++)}. So
   * stop reading at end() - 1.
   * @return The index into the array where data is no longer valid.
   */
  public int end();

  /**
   * The array of encoded data. This may be null or empty in which case 
   * {@link offset()} must equal {@link end()}.
   * @return The array of data, may be null or empty.
   */
  public long[] data();
  
  /**
   * Returns the units of the timestamp.
   * <b>NOTE:</b> This method doesn't check for null or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @return The units the timestamp is encoded in.
   */
  public static ChronoUnit timestampUnits(final long[] array, final int offset) {
    if ((array[offset] & NANOSECOND_FLAG) != 0) {
      return ChronoUnit.NANOS;
    } else if((array[offset] & MILLISECOND_FLAG) != 0) {
      return ChronoUnit.MILLIS;
    } else {
      return ChronoUnit.SECONDS;
    }
  }
  
  /**
   * Returns the timestamp in milliseconds, truncating the timestamp if it's in
   * nanoseconds.
   * <b>NOTE:</b> This method doesn't check for null or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @return The decoded timestamp in Unix Epoch millis.
   */
  public static long timestampInMillis(final long[] array, 
                                       final int offset) {
    return timestampInMillis(array, offset,
        timestampUnits(array, offset));
  }
  
  /**
   * Returns the timestamp in milliseconds, truncating the timestamp if it's in
   * nanoseconds.
   * <b>NOTE:</b> This method doesn't check for null or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @param units The units the timestamp is encoded in.
   * @return The decoded timestamp in Unix Epoch millis.
   */
  public static long timestampInMillis(final long[] array, 
                                       final int offset, 
                                       final ChronoUnit units) {
    switch (units) {
    case SECONDS:
      return (array[offset] & TIMESTAMP_MASK) * 1000L;
    case MILLIS:
      return array[offset] & TIMESTAMP_MASK;
    case NANOS:
      long base = (array[offset] & TIMESTAMP_MASK) * 1000L;
      base += array[offset + 1] / 1000000L;
      return base;
    default:
      throw new IllegalArgumentException("The units of " + units 
          + " are not supported.");
    }
  }
  
  /**
   * Updates the given timestamp with the nanosecond value.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @param timestamp The non-null timestamp to update.
   */
  public static void timestampInNanos(final long[] array, 
                                      final int offset, 
                                      final TimeStamp timestamp) {
    final ChronoUnit units = timestampUnits(array, offset);
    switch (units) {
    case SECONDS:
      timestamp.update(array[offset] & TIMESTAMP_MASK, 0);
      break;
    case MILLIS:
      final long ts = (array[offset] & TIMESTAMP_MASK) / 1000;
      final long millis = (array[offset] & TIMESTAMP_MASK) - (ts * 1000);
      timestamp.update(ts, millis * 1000000);
      break;
    case NANOS:
      timestamp.update(array[offset] & TIMESTAMP_MASK,
          array[offset + 1]);
      break;
      default:
        throw new IllegalArgumentException("The units of " + units 
            + " are not supported.");
    }
  }
  
  /**
   * Determines whether or not the value is encoded as a double in which case
   * you can call {@link #getDouble(long[], int, ChronoUnit)}. If false just
   * read the raw value as it's a long.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @return True if the value is encoded as a double, false if its a long.
   */
  public static boolean isDouble(final long[] array, final int offset) {
    return (array[offset] & FLOAT_FLAG) != 0;
  }
  
  /**
   * Decodes the value as a double.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @return The decoded double value.
   */
  public static double getDouble(final long[] array, 
                                 final int offset) {
    return getDouble(array, offset, timestampUnits(array, offset));
  }
  
  /**
   * Decodes the value as a double.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @param units The units the timestamp is encoded in.
   * @return The decoded double value.
   */
  public static double getDouble(final long[] array, 
                                 final int offset, 
                                 final ChronoUnit units) {
    return Double.longBitsToDouble(array[offset + 
                                         (units == ChronoUnit.NANOS ? 2 : 1)]);
  }
  
  /**
   * Fetches the long value at the proper offset.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @param units The units the timestamp is encoded in.
   * @return The long value.
   */
  public static long getLong(final long[] array, 
                             final int offset) {
    return getLong(array, offset, timestampUnits(array, offset));
  }
  
  /**
   * Fetches the long value at the proper offset.
   * <b>NOTE:</b> This method doesn't check for nulls or a valid index. Make sure
   * to do that before calling in.
   * @param array The non-null and non empty array to evaluate.
   * @param offset The offset into the array at the first timestamp for a value..
   * @param units The units the timestamp is encoded in.
   * @return The long value.
   */
  public static long getLong(final long[] array, 
                             final int offset, 
                             final ChronoUnit units) {
    return array[offset + (units == ChronoUnit.NANOS ? 2 : 1)];
  }
}
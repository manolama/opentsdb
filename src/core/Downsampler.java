// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.NoSuchElementException;


/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

  /** Function to use for downsampling. */
  private final Aggregator downsampler;
  /** Iterator to iterate the values of the current interval. */
  private final ValuesInInterval values_in_interval;
  // NOTE: Uses MutableDoubleDataPoint to reduce memory allocation overhead.
  /** The current downsample result. */
  //private MutableDoubleDataPoint data_point = new MutableDoubleDataPoint();
  private long timestamp;
  private double value;

  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler) {
    this.values_in_interval = new ValuesInInterval(source, interval_ms);
    this.downsampler = downsampler;
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  public boolean hasNext() {
    return values_in_interval.hasNextValue();
  }

  public DataPoint next() {
    if (hasNext()) {
      // get the value for the interval
      value = downsampler.runDouble(values_in_interval);
      
      timestamp = values_in_interval.getIntervalTimestamp();

//      data_point.reset(values_in_interval.getIntervalTimestamp(),
//                       downsample_value);
      
      // reset iterator to the next interval bucket
      values_in_interval.moveToNextInterval();
      
      // return the data
      return this;
    }
    throw new NoSuchElementException("no more data points in " + this);
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  public void seek(final long timestamp) {
    values_in_interval.seekInterval(timestamp);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Downsampler: ")
       .append("interval_ms=").append(values_in_interval.interval_ms)
       .append(", downsampler=").append(downsampler)
       .append(", current data=(").append(value)
       .append("), values_in_interval=").append(values_in_interval);
   return buf.toString();
  }

  /** Iterates source values for an interval. */
  private static class ValuesInInterval implements Aggregator.Doubles, DataPoint {

    /** The iterator of original source values. */
    private final SeekableView source;
    /** The sampling interval in milliseconds. */
    private final long interval_ms;
    /** The end of the current interval. */
    private long timestamp_end_interval = Long.MIN_VALUE;
    /** True if the last value was successfully extracted from the source. */
    private boolean has_next_value_from_source = false;
    /** The last data point extracted from the source. */
    //private MutableDoubleDataPoint next_dp = new MutableDoubleDataPoint();
    private DataPoint next_dp;
//    private long timestamp;
//    private double value;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     * @param source The iterator to access the underlying data.
     * @param interval_ms Downsampling interval.
     */
    ValuesInInterval(final SeekableView source, final long interval_ms) {
      this.source = source;
      this.interval_ms = interval_ms;
      this.timestamp_end_interval = interval_ms;
    }

    /** Initializes to iterate intervals. */
    private void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        resetEndOfInterval();
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        next_dp = source.next();
//        timestamp = dp.timestamp();
//        value = dp.toDouble();
        //next_dp.reset(source.next());
      } else {
        has_next_value_from_source = false;
      }
    }

    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source) {
        //long timestamp = next_dp.timestamp();
        timestamp_end_interval = calculateIntervalEndTimestamp(next_dp.timestamp());
      }
    }

    /** Moves to the next available interval. */
    void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    void seekInterval(long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp by the interval.
      source.seek(roundUpTimestamp(timestamp));
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    private long getIntervalTimestamp() {
      return calculateRepresentativeTimestamp(
          timestamp_end_interval - interval_ms);
    }

    /** Returns the end of the interval of the given timestamp. */
    private long calculateIntervalEndTimestamp(long timestamp) {
      return alignTimestamp(timestamp) + interval_ms;
    }

    /** Returns the timestamp of the interval of the given timestamp. */
    private long calculateRepresentativeTimestamp(long timestamp) {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      return alignTimestamp(timestamp);
    }

    /**
     * Rounds up the given timestamp.
     * @param timestamp Timestamp to round up
     * @return The smallest timestamp that is a multiple of the interval
     * and is greater than or equal to the given timestamp.
     */
    private long roundUpTimestamp(long timestamp) {
      return alignTimestamp(timestamp + interval_ms - 1);
    }

    /** Returns timestamp aligned by interval. */
    private long alignTimestamp(long timestamp) {
      return timestamp - (timestamp % interval_ms);
    }

    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      return has_next_value_from_source &&
          next_dp.timestamp() < timestamp_end_interval;
    }

    @Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double v = next_dp.toDouble();
        moveToNextValue();
        return v;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append("interval_ms=").append(interval_ms)
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }

    @Override
    public long timestamp() {
      // TODO Auto-generated method stub
      return next_dp.timestamp();
    }

    @Override
    public boolean isInteger() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public long longValue() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public double doubleValue() {
      // TODO Auto-generated method stub
      return next_dp.toDouble();
    }

    @Override
    public double toDouble() {
      // TODO Auto-generated method stub
      return next_dp.toDouble();
    }
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long longValue() {
    return 0;
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public double toDouble() {
    return value;
  }
}

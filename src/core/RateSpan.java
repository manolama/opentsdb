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
 * Iterator that generates rates from a sequence of adjacent data points.
 */
public class RateSpan implements SeekableView {

  /** A sequence of data points to compute rates. */
  private final SeekableView source;
  /** Options for calculating rates. */
  private final RateOptions options;
  /** The latter of two raw data points used to calculate the next rate. */
  private final MutableDataPoint next_data = new MutableDataPoint();
  /** The rate that will be returned at the {@link #next} call. */
  private final MutableDoubleDataPoint next_rate = new MutableDoubleDataPoint();
  /** Users see this rate after they called next. */
  private final MutableDoubleDataPoint prev_rate = new MutableDoubleDataPoint();
  /** True if it is initialized for iterating rates of changes. */
  private boolean initialized = false;

  /**
   * Constructs a {@link RateSpan} instance.
   * @param source The iterator to access the underlying data.
   * @param options Options for calculating rates.
   */
  RateSpan(final SeekableView source, final RateOptions options) {
    this.source = source;
    this.options = options;
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  /** @return True if there is a valid next value. */
  @Override
  public boolean hasNext() {
    initializeIfNotDone();
    return next_rate.timestamp() < Long.MAX_VALUE;
  }

  /**
   * @return the next rate of changes.
   * @throws NoSuchElementException if there is no more data.
   */
  @Override
  public DataPoint next() {
    initializeIfNotDone();
    if (hasNext()) {
      // NOTE: Just copies currentRate to prevRate, and does not allocate
      // any new DataPoint object to reduce the memory allocation overhead.
      // So, users access data at prev_rate.
      prev_rate.reset(next_rate);
      populateNextRate();
      return prev_rate;
    } else {
      throw new NoSuchElementException("no more values for " + toString());
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  @Override
  public void seek(long timestamp) {
    source.seek(timestamp);
    initialized = false;
  }

  // ---------------------- //
  // Private methods        //
  // ---------------------- //

  /** Initializes to iterate rate of changes. */
  private void initializeIfNotDone() {
    // NOTE: Delay initialization is needed to not access any data point
    // from source until a user requests it explicitly to avoid the
    // performance penalty by accessing the first data of a span.
    if (!initialized) {
      initialized = true;
      // NOTE: Calculates the first rate between the time zero and the first
      // data point for the backward compatibility.
      // TODO: Don't compute the first rate with the time zero.
      next_data.resetWithDoubleValue(0, 0);
      populateNextRate();
    }
  }

  /**
   * Populate to the next rate.
   */
  private void populateNextRate() {
    final MutableDataPoint prev_data = new MutableDataPoint();
    while (source.hasNext()) {
      prev_data.reset(next_data);
      next_data.reset(source.next());
      if (next_data.timestamp() <= prev_data.timestamp()) {
        throw new AssertionError("Foobar!");
      }
//      assert next_data.timestamp() > prev_data.timestamp(): 
//        ("Next timestamp (" + next_data.timestamp() + ") is supposed to be "
//          + " strictly greater than the previous one (" + 
//            prev_data.timestamp() + "), but it's"
//          + " not.  this=" + this);
        next_rate.reset(next_data.timestamp(), calculateRate(prev_data));
      return;
    }
    // The Long.MAX_VALUE timestamp means invalid value and works fine with
    // open-ended time ranges.
    next_rate.reset(Long.MAX_VALUE, 0);
  }

  /**
   * Calculates the rate between previous and current data points.
   */
  private double calculateRate(final MutableDataPoint prev_data) {
    final long t0 = prev_data.timestamp();
    final long t1 = next_data.timestamp();
    // TODO: for backwards compatibility we'll convert the ms to seconds
    // but in the future we should add a ratems flag that will calculate
    // the rate as is.
    final double time_delta_secs = ((double)(t1 - t0) / 1000.0);
    double difference;
    if (prev_data.isInteger() && next_data.isInteger()) {
      // NOTE: Calculates in the long type to avoid precision loss
      // while converting long values to double values if both values are long.
      // NOTE: Ignores the integer overflow.
      difference = next_data.longValue() - prev_data.longValue();
    }   else {
      difference = next_data.toDouble() - prev_data.toDouble();
    }
    
    if (options.isCounter() && difference < 0) {
      // NOTE: Assumes a roll over of a counter if we found that a counter value
      // was decreased while calculating a rate of changes for a counter.
      if (prev_data.isInteger() && next_data.isInteger()) {
        // NOTE: Calculates in the long type to avoid precision loss
        // while converting long values to double values if both values are long.
        difference = options.getCounterMax() - prev_data.longValue() +
            next_data.longValue();
      } else {
        difference = options.getCounterMax() - prev_data.toDouble() +
            next_data.toDouble();
      }
      // Assumes the count was reset if the calculated rate is larger than
      // the reset value, then returns 0 for the rate.
      final double r = time_delta_secs / time_delta_secs;
      if (options.getResetValue() > RateOptions.DEFAULT_RESET_VALUE
          && r > options.getResetValue()) {
        return 0.0;
      }
      return r;
    } else {
      return difference / time_delta_secs;
    }
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("RateSpan: ")
       .append(", options=").append(options)
       .append(", next_data=[").append(next_data)
       .append("], next_rate=[").append(next_rate)
       .append("], prev_rate=[").append(prev_rate)
       .append("], source=[").append(source).append("]");
    return buf.toString();
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
import java.util.TimeZone;

import net.opentsdb.utils.DateTime;

/**
 * Iterator that downsamples data points using an {@link Aggregator}.
 */
public class Downsampler implements SeekableView, DataPoint {

  static final long ONE_WEEK_INTERVAL = 604800000L;
  static final long ONE_MONTH_INTERVAL = 2592000000L;
  static final long ONE_YEAR_INTERVAL = 31536000000L;
  static final long ONE_DAY_INTERVAL = 86400000L;
  
  /** The downsampling specification when provided */
  protected final DownsamplingSpecification specification;
  
  /** The start timestamp of the actual query for use with "all" */
  protected final long query_start;
  
  /** The end timestamp of the actual query for use with "all" */
  protected final long query_end;
  
  /** The data source */
  protected final SeekableView source;
  
  /** Iterator to iterate the values of the current interval. */
  protected final ValuesInInterval values_in_interval;
  
  /** Last normalized timestamp */ 
  protected long timestamp;
  
  /** Last value as a double */
  protected double value;
  
  /** Whether or not to merge all DPs in the source into one vaalue */
  protected final boolean run_all;
  
  protected final TimeZone timezone;
  protected final boolean use_calendar;
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param interval_ms The interval in milli seconds wanted between each data
   * point.
   * @param downsampler The downsampling function to use.
   * @deprecated as of 2.3
   */
  Downsampler(final SeekableView source,
              final long interval_ms,
              final Aggregator downsampler) {
    this.source = source;
    values_in_interval = new ValuesInInterval();
    if (downsampler == Aggregators.NONE) {
      throw new IllegalArgumentException("cannot use the NONE "
          + "aggregator for downsampling");
    }
    specification = new DownsamplingSpecification(interval_ms, downsampler, 
        DownsamplingSpecification.DEFAULT_FILL_POLICY);
    query_start = 0;
    query_end = 0;
    run_all = false;
    timezone = TimeZone.getDefault();
    use_calendar = false;
  }
  
  /**
   * Ctor.
   * @param source The iterator to access the underlying data.
   * @param specification The downsampling spec to use
   * @param query_start The start timestamp of the actual query for use with "all"
   * @param query_end The end timestamp of the actual query for use with "all"
   * @since 2.3
   */
  Downsampler(final SeekableView source,
              final DownsamplingSpecification specification,
              final long query_start,
              final long query_end,
              final TimeZone timezone,
              final boolean use_calendar
      ) {
    this.source = source;
    this.specification = specification;
    values_in_interval = new ValuesInInterval();
    this.query_start = query_start;
    this.query_end = query_end;
    this.timezone = timezone;
    this.use_calendar = use_calendar;
    
    final String s = specification.getStringInterval();
    if (s != null && s.toLowerCase().contains("all")) {
      run_all = true;
    }  else {
      run_all = false;
    }
  }

  // ------------------ //
  // Iterator interface //
  // ------------------ //

  @Override
  public boolean hasNext() {
    return values_in_interval.hasNextValue();
  }

  /**
   * @throws NoSuchElementException if no data points remain.
   */
  @Override
  public DataPoint next() {
    if (hasNext()) {
      value = specification.getFunction().runDouble(values_in_interval);
      timestamp = values_in_interval.getIntervalTimestamp();
      values_in_interval.moveToNextInterval();
      return this;
    }
    throw new NoSuchElementException("no more data points in " + this);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // ---------------------- //
  // SeekableView interface //
  // ---------------------- //

  @Override
  public void seek(final long timestamp) {
    values_in_interval.seekInterval(timestamp);
  }

  // ------------------- //
  // DataPoint interface //
  // ------------------- //

  @Override
  public long timestamp() {
    if (run_all) {
      return query_start;
    }
    return timestamp;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long longValue() {
    throw new ClassCastException("Downsampled values are doubles");
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public double toDouble() {
    return value;
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Downsampler: ")
       .append(", downsampler=").append(specification)
       .append(", queryStart=").append(query_start)
       .append(", queryEnd=").append(query_end)
       .append(", runAll=").append(run_all)
       .append(", current data=(timestamp=").append(timestamp)
       .append(", value=").append(value)
       .append("), values_in_interval=").append(values_in_interval);
   return buf.toString();
  }

  /** Iterates source values for an interval. */
  protected class ValuesInInterval implements Aggregator.Doubles {

    /** The end of the current interval. */
    private long timestamp_end_interval = Long.MIN_VALUE;
    
    /** True if the last value was successfully extracted from the source. */
    private boolean has_next_value_from_source = false;
    
    /** The last data point extracted from the source. */
    private DataPoint next_dp = null;

    /** True if it is initialized for iterating intervals. */
    private boolean initialized = false;

    /**
     * Constructor.
     */
    protected ValuesInInterval() {
      if (run_all) {
        timestamp_end_interval = query_end;
      }
    }

    /** Initializes to iterate intervals. */
    protected void initializeIfNotDone() {
      // NOTE: Delay initialization is required to not access any data point
      // from the source until a user requests it explicitly to avoid the severe
      // performance penalty by accessing the unnecessary first data of a span.
      if (!initialized) {
        initialized = true;
        moveToNextValue();
        if (!run_all) {
          resetEndOfInterval();
        }
      }
    }

    /** Extracts the next value from the source. */
    private void moveToNextValue() {
      if (source.hasNext()) {
        has_next_value_from_source = true;
        // filter out dps that don't match start and end for run_alls
        if (run_all) {
          while (source.hasNext()) {
            next_dp = source.next();
            if (next_dp.timestamp() < query_start) {
              next_dp = null;
              continue;
            }
            if (next_dp.timestamp() >= query_end) {
              has_next_value_from_source = false;
            }
            break;
          }
          if (next_dp == null) {
            has_next_value_from_source = false;
          }
        } else {
          next_dp = source.next();
        }
      } else {
        has_next_value_from_source = false;
      }
    }

    /**
     * Resets the current interval with the interval of the timestamp of
     * the next value read from source. It is the first value of the next
     * interval. */
    private void resetEndOfInterval() {
      if (has_next_value_from_source && !run_all) {
        if (use_calendar && isCalendarInterval()) {
          timestamp_end_interval =  toEndOfInterval(next_dp.timestamp());
        }  else {
          // Sets the end of the interval of the timestamp.
          timestamp_end_interval = alignTimestamp(next_dp.timestamp()) + 
              specification.getInterval();
        }
      }
    }

    /** Moves to the next available interval. */
    void moveToNextInterval() {
      initializeIfNotDone();
      resetEndOfInterval();
    }

    /** Advances the interval iterator to the given timestamp. */
    void seekInterval(final long timestamp) {
      // To make sure that the interval of the given timestamp is fully filled,
      // rounds up the seeking timestamp to the smallest timestamp that is
      // a multiple of the interval and is greater than or equal to the given
      // timestamp..
      if (run_all) {
        source.seek(timestamp);
      } else if (use_calendar && isCalendarInterval()) {
        source.seek(alignTimestamp(timestamp + toEndOfInterval(timestamp)
            - toStartOfInterval(timestamp)));
      }  else {
        source.seek(alignTimestamp(timestamp + specification.getInterval() - 1));
      }
      initialized = false;
    }

    /** Returns the representative timestamp of the current interval. */
    protected long getIntervalTimestamp() {
      // NOTE: It is well-known practice taking the start time of
      // a downsample interval as a representative timestamp of it. It also
      // provides the correct context for seek.
      if (run_all) {
        return timestamp_end_interval;
      } else if (use_calendar && isCalendarInterval()) {
        return toStartOfInterval(timestamp_end_interval);
      }  else {
        return alignTimestamp(timestamp_end_interval - 
            specification.getInterval());
      }
    }

    /** Returns timestamp aligned by interval. */
    protected long alignTimestamp(final long timestamp) {
      if (use_calendar && isCalendarInterval()) {
        return toStartOfInterval(timestamp);
      }  else {
        return timestamp - (timestamp % specification.getInterval());
      }
    }

    /** Returns a flag denoting whether the interval can
     *  be aligned to the calendar */
    private boolean isCalendarInterval () {
      if (specification.getInterval() != 0 && 
         (specification.getInterval() % ONE_YEAR_INTERVAL == 0 ||
          specification.getInterval() % ONE_MONTH_INTERVAL == 0 ||
          specification.getInterval() % ONE_WEEK_INTERVAL == 0 ||
          specification.getInterval() % ONE_DAY_INTERVAL == 0)) {
        return true;
      }
      return false;
    }
    
    /** Returns a timestamp corresponding to the start of the interval
     *  in which the specified timestamp occurs, aligned to the calendar
     *  based on the timezone. */
    private long toStartOfInterval(long timestamp) {
      if (specification.getInterval() % ONE_YEAR_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_YEAR_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toStartOfYear(result, timezone) - 1;
        }
        return result + 1;
      } else if (specification.getInterval() % ONE_MONTH_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_MONTH_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toStartOfMonth(result, timezone) - 1;
        }
        return result + 1;
      } else if (specification.getInterval() % ONE_WEEK_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_WEEK_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toStartOfWeek(result, timezone) - 1;
        }
        return result + 1;
      } else if (specification.getInterval() % ONE_DAY_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_DAY_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toStartOfDay(result, timezone) - 1;
        }
        return result + 1;
      } else {
        throw new IllegalArgumentException(specification.getInterval() + 
            " does not correspond to a " +
         "an interval that can be aligned to the calendar.");
      }
    }

    /** Returns a timestamp corresponding to the end of the interval
     *  in which the specified timestamp occurs, aligned to the calendar
     *  based on the timezone. */
    private long toEndOfInterval(long timestamp) {
      if (specification.getInterval() % ONE_YEAR_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_YEAR_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toEndOfYear(result, timezone) + 1;
        }
        return result - 1;
      } else if (specification.getInterval() % ONE_MONTH_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_MONTH_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toEndOfMonth(result, timezone) + 1;
        }
        return result - 1;
      } else if (specification.getInterval() % ONE_WEEK_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_WEEK_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toEndOfWeek(result, timezone) + 1;
        }
        return result - 1;
      } else if (specification.getInterval() % ONE_DAY_INTERVAL == 0) {
        final long multiplier = specification.getInterval() / ONE_DAY_INTERVAL;
        long result = timestamp;
        for (long i = 0; i < multiplier; i++) {
          result = DateTime.toEndOfDay(result, timezone) + 1;
        }
        return result - 1;
      } else {
        throw new IllegalArgumentException(specification.getInterval() + 
            " does not correspond to a " +
         "an interval that can be aligned to the calendar.");
      }
    }
    
    // ---------------------- //
    // Doubles interface //
    // ---------------------- //

    @Override
    public boolean hasNextValue() {
      initializeIfNotDone();
      if (run_all) {
        return has_next_value_from_source;
      }
      return has_next_value_from_source &&
          next_dp.timestamp() < timestamp_end_interval;
    }

    @Override
    public double nextDoubleValue() {
      if (hasNextValue()) {
        double value = next_dp.toDouble();
        moveToNextValue();
        return value;
      }
      throw new NoSuchElementException("no more values in interval of "
          + timestamp_end_interval);
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder();
      buf.append("ValuesInInterval: ")
         .append(", timestamp_end_interval=").append(timestamp_end_interval)
         .append(", has_next_value_from_source=")
         .append(has_next_value_from_source);
      if (has_next_value_from_source) {
        buf.append(", nextValue=(").append(next_dp).append(')');
      }
      buf.append(", source=").append(source);
      return buf.toString();
    }
  }
}

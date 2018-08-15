// This file is part of OpenTSDB.
// Copyright (C) 2014-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.interpolation.QueryInterpolator;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;

/**
 * Iterator that downsamples data points using an {@link Aggregator} following
 * various rules:
 * <ul>
 * <li>If {@link DownsampleConfig#fill()} is enabled, then a value is emitted
 * for every timestamp between {@link DownsampleConfig#start()} and 
 * {@link DownsampleConfig#end()} inclusive. Otherwise only values that are 
 * not null or {@link Double#isNaN()} will be emitted.</li>
 * <li>If the source time series does not have any real values (non-null or filled)
 * or the values are outside of the query bounds set in the config, then the 
 * iterator will false for {@link #hasNext()} even if filling is enabled.</li>
 * <li>Values emitted from this iterator are inclusive of the config 
 * {@link DownsampleConfig#start()} and {@link DownsampleConfig#end()} timestamps.
 * <li>Value timestamps emitted from the iterator are aligned to the <b>top</b>
 * of the interval. E.g. if the interval is set to 1 day, then the timestamp will
 * always be the midnight hour at the start of the day and includes values from
 * [midnight of day, midnight of next day). This implies:
 * <ul>
 * <li>If a source timestamp is earlier than the {@link DownsampleConfig#start()}
 * it will not be included in the results even though the query may have a start
 * timestamp earlier than {@link DownsampleConfig#start()} (due to the fact that
 * the config will snap to the earliest interval greater than or equal to the
 * query start timestamp.</li>
 * <li>If a source timestamp is later than the {@link DownsampleConfig#end()}
 * time but is within the interval defined by {@link DownsampleConfig#end()},
 * it <b>will</b> be included in the results.</li>
 * </ul></li>
 * </ul>
 * <p>
 * Note that in order to optimistically take advantage of special instruction
 * sets on CPUs, we dump values into an array as we downsample and grow the 
 * array as needed, never shrinking it or deleting it. We assume that values
 * are longs until we encounter a double at which point we switch to an alternate
 * array and copy the longs over. So there is potential here that two big
 * arrays could be created but in generally there should only be a few of these
 * iterators instantiated at any time for a query.
 * <p>
 * This combines the old filling downsampler and downsampler classes from 
 * OpenTSDB 2.x.
 * <p>
 * <b>WARNING:</b> For now, the arrays grow by doubling. That means there's a 
 * potential for eating up a ton of heap if there are massive amounts of values
 * (e.g. nano second data points) in an interval. 
 * TODO - look at a better way of growing the arrays.
 * @since 3.0
 */
public class DownsampleNumericToArrayIterator implements QueryIterator,
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType {

  private final QueryNode node;
  private int width;
  
  /** The result we belong to. */
  private final DownsampleResult result;
  
  /** The downsampler config. */
  private final DownsampleConfig config;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The source to pull an iterator from. */
  private final TimeSeries source;
  
  /** The interpolator to use for filling missing intervals. */
  private final QueryInterpolator<NumericType> interpolator;
  
  /** The current interval timestamp marking the start of the interval. */
  private TimeStamp interval_ts;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  private long[] long_values;
  private double[] double_values;
  
  /**
   * Default ctor. This will seek to the proper source timestamp.
   * 
   * @param node A non-null query node to pull the config from.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required argument is missing.
   */
  @SuppressWarnings("unchecked")
  public DownsampleNumericToArrayIterator(final QueryNode node, 
                                          final QueryResult result,
                                          final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (result == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    
    this.node = node;
    this.result = (DownsampleResult) result;
    this.source = source;
    aggregator = Aggregators.get(((DownsampleConfig) node.config()).aggregator());
    config = (DownsampleConfig) node.config();
    final QueryInterpolatorConfig interpolator_config = config.interpolatorConfig(NumericType.TYPE);
    if (interpolator_config == null) {
      throw new IllegalArgumentException("No interpolator config found for type");
    }
    
    final QueryInterpolatorFactory factory = node.pipelineContext()
        .tsdb().getRegistry().getPlugin(QueryInterpolatorFactory.class, 
                                        interpolator_config.id());
    if (factory == null) {
      throw new IllegalArgumentException("No interpolator factory found for: " + 
          interpolator_config.interpolatorType() == null ? "Default" : 
            interpolator_config.interpolatorType());
    }
    
    final QueryInterpolator<?> interp = factory.newInterpolator(
        NumericType.TYPE, 
        (Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>) new Downsampler(),
        interpolator_config);
    if (interp == null) {
      throw new IllegalArgumentException("No interpolator implementation found for: " + 
          interpolator_config.interpolatorType() == null ? "Default" : 
            interpolator_config.interpolatorType());
    }
    interpolator = (QueryInterpolator<NumericType>) interp;
    interval_ts = this.result.start().getCopy();
    if (interpolator.hasNext()) {
      has_next = true;
    }
//    if (config.fill() && !config.runAll()) {
//      if (!interpolator.hasNext()) {
//        has_next = false;
//      } else {
//        // make sure there is a value within our query interval
//        TimeStamp interval_before_last = this.result.end().getCopy();
//        this.result.nextTimestamp(interval_before_last);
//        if (interpolator.nextReal().compare(Op.GTE, interval_before_last) ||
//            interpolator.nextReal().compare(Op.LT, this.result.start())) {
//          has_next = false;
//        } else {
//          value = interpolator.next(interval_ts);
//          has_next = true;
//        }
//      }
//    } else {
//      if (interpolator.hasNext()) {
//        value = interpolator.next(interval_ts);
//        while (value != null && (value.value() == null || 
//            (!value.value().isInteger() && Double.isNaN(value.value().doubleValue())))) {
//          if (interpolator.hasNext()) {
//            this.result.nextTimestamp(interval_ts);
//            if (interval_ts.compare(Op.GT, this.result.end())) {
//              value = null;
//              break;
//            }
//            value = interpolator.next(interval_ts);
//          } else {
//            value = null;
//          }
//        }
//        
//        if (value != null) {
//          has_next = true;
//        }
//      }
//    }
//    response = new MutableNumericValue();
    
    if (has_next) {
      final TimeStamp ts;
      if (((DownsampleConfig) node.config()).startTime() == null) {
        ts = ((SemanticQuery) node.pipelineContext().query()).startTime();
        ts.snapToPreviousInterval(((DownsampleConfig) node.config()).intervalPart(), ((DownsampleConfig) node.config()).units());;
      } else {
        ts = ((DownsampleConfig) node.config()).startTime().getCopy();
      }
      final TimeStamp end;
      if (((DownsampleConfig) node.config()).endTime() == null) {
        end = ((SemanticQuery) node.pipelineContext().query()).endTime();
      } else {
        end = ((DownsampleConfig) node.config()).endTime();
      }
      while (ts.compare(Op.LTE, end)) {
        width++;
        ts.add(((DownsampleConfig) node.config()).interval());
      }
    }
    
    System.out.println(" GONNA DO DS TO ARRAY!!!!!!!!!!!!!  " + has_next + "  ID: " + source.id());
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeStamp timestamp() {
    return result.start();
  }
  
  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    has_next = false;
    return this;
  }
  
  @Override
  public NumericArrayType value() {
    long_values = new long[width];
    
    TimeSeriesValue<NumericType> value;
    int idx = 0;
    while (idx < width) {
      value = interpolator.next(interval_ts);
      if (value.value().isInteger() && long_values != null) {
        long_values[idx] = value.value().longValue();
      } else {
        if (double_values == null) {
          double_values = new double[long_values.length];
          for (int i = 0; i < idx; i++) {
            double_values[i] = long_values[i];
          }
          long_values = null;
        }
        double_values[idx] = value.value().toDouble();
      }
      //System.out.println("TS: " + interval_ts);
      interval_ts.add(((DownsampleConfig) node.config()).interval());
      idx++;
    }
    
    System.out.println("VALUE: " + Arrays.toString(long_values) + "  " + Arrays.toString(double_values));
    return this;
  }
  
  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean isInteger() {
    return long_values == null ? false : true;
  }

  @Override
  public long[] longArray() {
    return long_values;
  }

  @Override
  public double[] doubleArray() {
    return double_values;
  }
  
  /**
   * A class that actually performs the downsampling calculation on real
   * values from the source timeseries. It's a child class so we share the same
   * reference for the config and source.
   */
  private class Downsampler implements 
      Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {
    /** The last data point extracted from the source. */
    private TimeSeriesValue<NumericType> next_dp = null;
    
    /** The data point set and returned by the iterator. */
    private final MutableNumericValue dp;
      
    /** An array of long values used when all sources return longs. */
    private long[] long_values;
    
    /** An array of double values used when one or more sources return a double. */
    private double[] double_values;
    
    /** Used as an index into the value arrays at any given iteration. */
    private int value_idx;
    
    /** Whether or not another real value is present. True while at least one 
     * of the time series has a real value. */
    private boolean has_next = false;
    
    /** The current interval start timestamp. */
    private TimeStamp interval_start;
    
    /** The current interval end timestamp. */
    private TimeStamp interval_end;
    
    /** The iterator pulled from the source. */
    private final Iterator<TimeSeriesValue<?>> iterator;
    
    /**
     * Default ctor.
     */
    @SuppressWarnings("unchecked")
    Downsampler() {
      interval_start = result.start().getCopy();
      if (config.runAll()) {
        interval_end = result.end().getCopy();
      } else {
        interval_end = result.start().getCopy();
        result.nextTimestamp(interval_end);
      }
      
      final Optional<Iterator<TimeSeriesValue<?>>> optional = 
          source.iterator(NumericType.TYPE);
      if (optional.isPresent()) {
        iterator = optional.get();
      } else {
        iterator = null;
      }
      if (iterator.hasNext()) {
        next_dp = (TimeSeriesValue<NumericType>) iterator.next();
      }
      
      dp = new MutableNumericValue();
      has_next = iterator.hasNext();
      long_values = new long[2];
      
      // blow out anything earlier than the first timestamp
      if (next_dp != null) {
        // out of bounds
        if (next_dp.timestamp().compare(Op.GT, result.end())) {
          next_dp = null;
        }
        
        while (next_dp != null && next_dp.value() != null && 
            next_dp.timestamp().compare(Op.LT, interval_start)) {
          if (iterator.hasNext()) {
            next_dp = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            next_dp = null;
          }
        }
      }
      
      has_next = next_dp != null;
    }
    
    /**
     * Helper that expands the array as necessary.
     * @param value A value to store.
     */
    private void add(final long value) {
      if (value_idx >= long_values.length) {
        final long[] temp = new long[long_values.length * 2];
        System.arraycopy(long_values, 0, temp, 0, long_values.length);
        long_values = temp;
      }
      long_values[value_idx++] = value;
    }
    
    /**
     * Helper that expands the array as necessary.
     * @param value
     */
    private void add(final double value) {
      if (value_idx >= double_values.length) {
        final double[] temp = new double[double_values.length * 2];
        System.arraycopy(double_values, 0, temp, 0, value_idx - 1);
        double_values = temp;
      }
      double_values[value_idx++] = value;
    }
      
    /**
     * Helper that moves all of the longs to the doubles array.
     */
    private void shiftToDouble() {
      if (double_values == null) {
        double_values = new double[long_values.length];
      }
      if (value_idx == 0) {
        return;
      }
      for (int i = 0; i < value_idx; i++) {
        double_values[i] = (double) long_values[i];
      }
    }
    
    @Override
    public boolean hasNext() {
      return has_next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TimeSeriesValue<NumericType> next() {
      if (!has_next) {
        throw new RuntimeException("FAIL! NO more data");
      }
      has_next = false;
      value_idx = 0;
      boolean longs = true;
      
      // we only return reals here, so skip empty intervals. Those are handled by
      // the interpolator.
      while (true) {
        if (next_dp == null) {
          break;
        }
        
        if (config.runAll() || 
            next_dp.timestamp().compare(Op.LT, interval_end)) {
          // when running through all the dps, make sure we don't go over the 
          // end timestamp of the query.
          if (config.runAll() && 
              next_dp.timestamp().compare(Op.GT, interval_end)) {
            next_dp = null;
            break;
          }
          
          if (next_dp.value() != null && !next_dp.value().isInteger() && 
              Double.isNaN(next_dp.value().doubleValue())) {
            if (config.infectiousNan()) {
              longs = false;
              shiftToDouble();
              add(Double.NaN);
            }
          } else if (next_dp.value() != null) {
            if (next_dp.value().isInteger() && longs) {
              add(next_dp.value().longValue());
            } else {
              if (longs) {
                longs = false;
                shiftToDouble();
              }
              add(next_dp.value().toDouble());
            }
          }
          
          if (iterator.hasNext()) {
            next_dp = (TimeSeriesValue<NumericType>) iterator.next();
          } else {
            next_dp = null;
          }
        } else if (value_idx == 0) {
          result.nextTimestamp(interval_start);
          result.nextTimestamp(interval_end);
          if (interval_start.compare(Op.GT, result.end())) {
            next_dp = null;
            break;
          }
        } else {
          // we've reached the end of an interval and have data.
          break;
        }
      }
      
      if (value_idx < 1) {
        dp.reset(interpolator.next(interval_ts));
      } else if (longs) {
        dp.resetTimestamp(interval_start);
        aggregator.run(long_values, 0, value_idx, dp);
      } else {
        dp.resetTimestamp(interval_start);
        aggregator.run(double_values, 0, value_idx, false/* TODO -!! */, dp);
      }
      
      result.nextTimestamp(interval_start);
      result.nextTimestamp(interval_end);
      if (interval_start.compare(Op.GT, result.end())) {
        next_dp = null;
      }
      has_next = next_dp != null;
      return dp;
    }
  }
}

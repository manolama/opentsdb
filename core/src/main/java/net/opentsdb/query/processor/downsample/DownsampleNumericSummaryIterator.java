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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.data.types.numeric.Aggregators;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericAccumulator;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;

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
public class DownsampleNumericSummaryIterator implements QueryIterator {
  /** The downsampler config. */
  private final DownsampleConfig config;
    
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The source to pull an iterator from. */
  private final TimeSeries source;
  
  /** The interpolator to use for filling missing intervals. */
  private final QueryIteratorInterpolator<NumericSummaryType> interpolator;
  
  /** The current interval timestamp marking the start of the interval. */
  private TimeStamp interval_ts;
  
  /** Whether or not the iterator has another real or filled value. */
  private boolean has_next;
  
  private final NumericSummaryInterpolatorConfig interpolator_config;
  
  MutableNumericSummaryValue dp = new MutableNumericSummaryValue();
  
  /**
   * Default ctor. This will seek to the proper source timestamp.
   * 
   * @param node A non-null query node to pull the config from.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required argument is missing.
   */
  @SuppressWarnings("unchecked")
  public DownsampleNumericSummaryIterator(final QueryNode node, 
                                          final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (node.config() == null) {
      throw new IllegalArgumentException("Node config cannot be null.");
    }
    this.source = source;
    aggregator = Aggregators.get(((DownsampleConfig) node.config()).aggregator());
    config = (DownsampleConfig) node.config();
    
    //NumericSummaryInterpolatorConfig ic = (NumericSummaryInterpolatorConfig) config.interpolationConfig().config(NumericSummaryType.TYPE);
    interpolator_config = (NumericSummaryInterpolatorConfig) config.interpolationConfig().config(NumericSummaryType.TYPE);
    interpolator = (QueryIteratorInterpolator<NumericSummaryType>) config.interpolationConfig()
        .newInterpolator(NumericSummaryType.TYPE, new Downsampler());
    interval_ts = config.start().getCopy();
    
    // check bounds
    if (interpolator.hasNext()) {
      if (interpolator.nextReal().compare(RelationalOperator.GTE, config.start()) && 
          interpolator.nextReal().compare(RelationalOperator.LT, config.end())) {
        has_next = true;
      }
      
      if (!config.fill()) {
        // advance to the first real value
        while (interpolator.nextReal().compare(RelationalOperator.GT, interval_ts)) {
          config.nextTimestamp(interval_ts);
        }
      }
    }
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }
  
  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    dp.reset(interpolator.next(interval_ts));
    System.out.println("[DS] next valu: " + dp);
    config.nextTimestamp(interval_ts);
    has_next = false;
    if (config.fill() && !config.runAll()) {
      if (interval_ts.compare(RelationalOperator.GTE, config.end())) {
        has_next = false;
      } else {
        has_next = true;
      }
    } else if (interpolator.hasNext()) {
      if (interpolator.nextReal().compare(RelationalOperator.GTE, config.start()) && 
          interpolator.nextReal().compare(RelationalOperator.LT, config.end())) {
        while (interpolator.nextReal().compare(RelationalOperator.GT, interval_ts)) {
          config.nextTimestamp(interval_ts);
        }
        has_next = true;
      }
    }
    
    return dp;
  }
  
  /**
   * A class that actually performs the downsampling calculation on real
   * values from the source timeseries. It's a child class so we share the same
   * reference for the config and source.
   */
  protected class Downsampler implements  
      Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> {
    /** The last data point extracted from the source. */
    private TimeSeriesValue<NumericSummaryType> next_dp = null;
    
    /** The data point set and returned by the iterator. */
    private final MutableNumericSummaryValue dp;
    
    private final Map<Integer, NumericAccumulator> accumulators;
    
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
      interval_start = config.start().getCopy();
      if (config.runAll()) {
        interval_end = config.end().getCopy();
      } else {
        interval_end = config.start().getCopy();
        config.nextTimestamp(interval_end);
      }
      
      final Optional<Iterator<TimeSeriesValue<?>>> optional = 
          source.iterator(NumericSummaryType.TYPE);
      if (optional.isPresent()) {
        iterator = optional.get();
      } else {
        iterator = null;
        dp = null;
        has_next = false;
        accumulators = null;
        return;
      }
      
      dp = new MutableNumericSummaryValue();

      while (iterator.hasNext()) {
        next_dp = (TimeSeriesValue<NumericSummaryType>) iterator.next();
        if (next_dp != null && 
            next_dp.timestamp().compare(RelationalOperator.GTE, interval_start) && 
            next_dp.value() != null && 
            next_dp.value().summariesAvailable().size() > 0 &&
            next_dp.timestamp().compare(RelationalOperator.LT, config.end())) {
          break;
        } else {
          next_dp = null;
        }
      }
      
      has_next = next_dp != null;
      accumulators = Maps.newHashMapWithExpectedSize(2);
    }
    
    @Override
    public boolean hasNext() {
      return has_next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TimeSeriesValue<NumericSummaryType> next() {
      if (!has_next) {
        throw new RuntimeException("FAIL! NO more data");
      }
      has_next = false;
      resetIndices();
      
      // we only return reals here, so skip empty intervals. Those are handled by
      // the interpolator.
      boolean data_in_iteration = false;
      while (true) {
        if (next_dp == null) {
          break;
        }
        
        if (config.runAll() || 
            next_dp.timestamp().compare(config.runAll() ? 
                RelationalOperator.LTE : RelationalOperator.LT, interval_end)) {
          // when running through all the dps, make sure we don't go over the 
          // end timestamp of the query.
          if (config.runAll() && 
              next_dp.timestamp().compare(config.runAll() ? 
                  RelationalOperator.GT : RelationalOperator.GTE, interval_end)) {
            next_dp = null;
            break;
          }
          
          if (next_dp.value() != null) {
            for (final int summary : next_dp.value().summariesAvailable()) {
              final NumericType value = next_dp.value().value(summary);
              if (value == null) {
                continue;
              }
              
              NumericAccumulator accumulator = accumulators.get(summary);
              if (accumulator == null) {
                accumulator = new NumericAccumulator();
                accumulators.put(summary, accumulator);
              }
             
              if (!value.isInteger() && 
                  Double.isNaN(value.doubleValue())) {
                if (config.infectiousNan()) {
                  accumulator.add(Double.NaN);
                }
              } else if (value != null) {
                if (value.isInteger()) {
                  accumulator.add(value.longValue());
                } else {
                  accumulator.add(value.toDouble());
                }
              }
              data_in_iteration = true;
            }
          }
          
          if (iterator.hasNext()) {
            while (iterator.hasNext()) {
              next_dp = (TimeSeriesValue<NumericSummaryType>) iterator.next();
              if (next_dp != null && 
                  next_dp.value() != null && 
                  next_dp.value().summariesAvailable().size() > 0) {
                break;
              } else {
                next_dp = null;
              }
            }
          } else {
            next_dp = null;
          }
        } else if (!data_in_iteration) {
          config.nextTimestamp(interval_start);
          config.nextTimestamp(interval_end);
          if (interval_start.compare(config.runAll()? 
              RelationalOperator.GT : RelationalOperator.GTE, config.end())) {
            next_dp = null;
            break;
          }
        } else {
          // we've reached the end of an interval and have data.
          break;
        }
      }
      
      if (aggregator.name().equals("avg")) {
        for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() > 0) {
            accumulator.run(interpolator_config.componentAggregator() != null ? interpolator_config.componentAggregator() : aggregator, false /* TODO */);
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        
        // TODO - this is an ugly old hard-coding!!! Make it flexible somehow
        final NumericType sum = dp.value(interpolator_config.rollupConfig().getIdForAggregator("sum"));
        final NumericType count = dp.value(interpolator_config.rollupConfig().getIdForAggregator("count"));
        dp.clear();
        if (sum == null || count == null) {
          // no-op
          System.out.println("DAMN, missing a sum " + sum + " or count + " + count);
        } else {
          dp.resetValue(interpolator_config.rollupConfig().getIdForAggregator("avg"), (sum.toDouble() / count.toDouble()));
        }
        dp.resetTimestamp(interval_start);
      } else if (aggregator.name().equals("count")) {
        for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() > 0) {
            
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        dp.resetTimestamp(interval_start);
      } else {
        for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
          final NumericAccumulator accumulator = entry.getValue();
          if (accumulator.valueIndex() < 1) {
            dp.nullValue(entry.getKey());
          } else {
            if (aggregator.name().equals("count") && entry.getKey() == interpolator_config.rollupConfig().getIdForAggregator("count")) {
              accumulator.run(interpolator_config.componentAggregator() != null ? interpolator_config.componentAggregator() : aggregator, false /* TODO */);
            } else {
              accumulator.run(aggregator, false /** TODO */);
            }
            dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
          }
        }
        dp.resetTimestamp(interval_start);
      }
      
      config.nextTimestamp(interval_start);
      config.nextTimestamp(interval_end);
      if (interval_start.compare(config.runAll() ? 
          RelationalOperator.GT : RelationalOperator.GTE, config.end())) {
        next_dp = null;
      }
      has_next = next_dp != null;
      return dp;
    }

    private void resetIndices() {
      for (final NumericAccumulator accumulator : accumulators.values()) {
        accumulator.reset();
      }
      dp.clear();
    }
  }
}

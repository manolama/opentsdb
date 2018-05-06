package net.opentsdb.query.processor.groupby;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
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

public class GroupByNumericSummaryIterator implements QueryIterator, 
  TimeSeriesValue<NumericSummaryType>{

  /** Whether or not NaNs are sentinels or real values. */
  private final boolean infectious_nan;
  
  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** The next timestamp to return. */
  private final TimeStamp next_ts = new MillisecondTimeStamp(0);
  
  /** The next timestamp evaulated when returning the next value. */
  private final TimeStamp next_next_ts = new MillisecondTimeStamp(0);
  
  /** The data point set and returned by the iterator. */
  private final MutableNumericSummaryValue dp;
  
  /** The list of interpolators containing the real sources. */
  private final QueryIteratorInterpolator<NumericSummaryType>[] interpolators;
  
  private final Map<Integer, NumericAccumulator> accumulators;
  
  private final NumericSummaryInterpolatorConfig config;
  
  /** An index in the sources array used when pulling numeric iterators from the
   * sources. Must be less than or equal to the number of sources. */
  private int iterator_max;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  public GroupByNumericSummaryIterator(final QueryNode node, 
      final Map<String, TimeSeries> sources) {
    this(node, sources == null ? null : sources.values());
  }
  
  public GroupByNumericSummaryIterator(final QueryNode node, 
      final Collection<TimeSeries> sources) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (sources == null) {
      throw new IllegalArgumentException("Sources cannot be null.");
    }
    if (sources.isEmpty()) {
      throw new IllegalArgumentException("Sources cannot be empty.");
    }
    if (Strings.isNullOrEmpty(((GroupByConfig) node.config()).getAggregator())) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty."); 
    }
    dp = new MutableNumericSummaryValue();
    next_ts.setMax();
    // TODO - better way of supporting aggregators
    aggregator = Aggregators.get(((GroupByConfig) node.config()).getAggregator());
    infectious_nan = ((GroupByConfig) node.config()).getInfectiousNan();
    interpolators = new QueryIteratorInterpolator[sources.size()];
    for (final TimeSeries source : sources) {
      if (source == null) {
        throw new IllegalArgumentException("Null time series are not allowed in the sources.");
      }
      
      interpolators[iterator_max] = (QueryIteratorInterpolator<NumericSummaryType>) 
          ((GroupByConfig) node.config()).interpolationConfig().newInterpolator(NumericSummaryType.TYPE, source);
      if (interpolators[iterator_max].hasNext()) {
        has_next = true;
        if (interpolators[iterator_max].nextReal().compare(RelationalOperator.LT, next_ts)) {
          next_ts.update(interpolators[iterator_max].nextReal());
        }
      }
      iterator_max++;
    }
    
    config = (NumericSummaryInterpolatorConfig) ((GroupByConfig) node.config()).interpolationConfig().config(NumericSummaryType.TYPE);
    accumulators = Maps.newHashMapWithExpectedSize(config.expectedSummaries().size());
    for (final int summary : config.expectedSummaries()) {
      accumulators.put(summary, new NumericAccumulator());
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    has_next = false;
    next_next_ts.setMax();
    dp.clear();
    resetIndices();
    
    for (int i = 0; i < iterator_max; i++) {
      final TimeSeriesValue<NumericSummaryType> v = interpolators[i].next(next_ts);
      if (v == null || v.value() == null) {
        // skip it
      } else {
        for (final int summary : v.value().summariesAvailable()) {
          final NumericType value = v.value().value(summary);
          if (value == null) {
            continue;
          }
          
          NumericAccumulator accumulator = accumulators.get(summary);
          if (accumulator == null) {
            // TODO - counter about the unexpected summary
            System.out.println("WTF? Data we didn't expect: " + summary);
            continue;
          }
          
          if (value.isInteger()) {
            accumulator.add(value.longValue());
          } else {
            accumulator.add(value.doubleValue());
          }
        }
      }
      
      if (interpolators[i].hasNext()) {
        has_next = true;
        if (interpolators[i].nextReal().compare(RelationalOperator.LT, next_next_ts)) {
          next_next_ts.update(interpolators[i].nextReal());
        }
      }
    }
    
    if (aggregator.name().equals("avg") && !config.expectedSummaries().contains(config.rollupConfig().getIdForAggregator("avg"))) {
      for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
        final NumericAccumulator accumulator = entry.getValue();
        if (accumulator.valueIndex() > 0) {
          accumulator.run(config.componentAggregator() != null ? config.componentAggregator() : aggregator, infectious_nan);
          dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
        }
      }
      // TODO - this is an ugly old hard-coding!!! Make it flexible somehow
      final NumericType sum = dp.value(config.rollupConfig().getIdForAggregator("sum"));
      final NumericType count = dp.value(config.rollupConfig().getIdForAggregator("count"));
      dp.clear();
      if (sum == null || count == null) {
        // no-op
      } else {
        dp.resetValue(config.rollupConfig().getIdForAggregator("avg"), (sum.toDouble() / count.toDouble()));
      }
      dp.resetTimestamp(next_ts);
    } else {
      for (final Entry<Integer, NumericAccumulator> entry : accumulators.entrySet()) {
        final NumericAccumulator accumulator = entry.getValue();
        if (accumulator.valueIndex() > 0) {
          accumulator.run(aggregator, infectious_nan);
          dp.resetValue(entry.getKey(), (NumericType) accumulator.dp());
        }
      }
      dp.resetTimestamp(next_ts);
    }
    System.out.println("[GB] next valu: " + dp);
    next_ts.update(next_next_ts);
    return this;
  }

  @Override
  public TimeStamp timestamp() {
    return dp.timestamp();
  }

  @Override
  public NumericSummaryType value() {
    return dp.value();
  }

  @Override
  public TypeToken<NumericSummaryType> type() {
    return NumericSummaryType.TYPE;
  }

  private void resetIndices() {
    for (final NumericAccumulator accumulator : accumulators.values()) {
      accumulator.reset();
    }
  }
}

package net.opentsdb.query.processor.downsample;

import java.util.Iterator;
import java.util.Optional;

import com.google.common.base.Strings;
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
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;
import net.opentsdb.query.processor.groupby.GroupByConfig;

public class DownsampleNumericArrayIterator implements QueryIterator, 
  TimeSeriesValue<NumericArrayType>, 
  NumericArrayType {

  /** The aggregator. */
  private final NumericAggregator aggregator;
  
  /** Whether or not another real value is present. True while at least one 
   * of the time series has a real value. */
  private boolean has_next = false;
  
  private final QueryNode node;
  private final DownsampleResult result;
  private int width;
  
  private final Iterator<TimeSeriesValue<?>> iterator;
  
  private long[] long_values;
  private double[] double_values;
  
  /**
   * Ctor with a collection of source time series.
   * @param node The non-null node this iterator belongs to.
   * @param result The result this source is a part of.
   * @param source A non-null source to pull numeric iterators from. 
   * @throws IllegalArgumentException if a required parameter or config is 
   * not present.
   */
  public DownsampleNumericArrayIterator(final QueryNode node, 
                                        final QueryResult result,
                                        final TimeSeries source) {
    if (node == null) {
      throw new IllegalArgumentException("Query node cannot be null.");
    }
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    if (Strings.isNullOrEmpty(((GroupByConfig) node.config()).getAggregator())) {
      throw new IllegalArgumentException("Aggregator cannot be null or empty."); 
    }
    this.node = node;
    this.result = (DownsampleResult) result;
    
    // TODO - better way of supporting aggregators
    aggregator = Aggregators.get(((DownsampleConfig) node.config()).aggregator());
    final Optional<Iterator<TimeSeriesValue<?>>> optional = 
        source.iterator(NumericArrayType.TYPE);
    if (optional.isPresent()) {
      iterator = optional.get();
      has_next = iterator.hasNext();
      final TimeStamp ts = ((DownsampleConfig) node.config()).startTime().getCopy();
      while (ts.compare(Op.LTE, ((DownsampleConfig) node.config()).endTime())) {
        width++;
      }
      
    } else {
      iterator = null;
    }
  }
  
  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<? extends TimeSeriesDataType> next() {
    return this;
  }

  @Override
  public TimeStamp timestamp() {
    return result.start();
  }

  @Override
  public NumericArrayType value() {
    final TimeSeriesValue<NumericArrayType> value =
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    
    // TODO - also depends on the agg method if we return a double or long
    long[] long_source = null;
    double[] double_source = null;
    
    if (value.value().isInteger()) {
      long_values = new long[width];
      long_source = value.value().longArray();
    } else {
      double_values = new double[width];
      double_source = value.value().doubleArray();
    }
    
    final TimeStamp end_of_interval = ((DownsampleConfig) node.config()).startTime().getCopy();
    TimeStamp source_ts = result.sourceResult().timeSpecification().start();
    int source_idx = 0;
    int agg_idx_start = -1;
    int agg_idx_end = 0;
    final MutableNumericValue dp = new MutableNumericValue(end_of_interval, 0L); 
    for (int i = 0; i < width; i++) {
      end_of_interval.add(((DownsampleConfig) node.config()).duration());
      while (source_ts.compare(Op.LTE, end_of_interval)) {
        if (agg_idx_start < 0) {
          agg_idx_start = agg_idx_end = source_idx;
        } else {
          agg_idx_end++;
        }
        source_ts.add(result.sourceResult().timeSpecification().interval());
        source_idx++;
      }
      
      if (agg_idx_start < 0) {
        // TODO proper fill!
      } else if (long_source != null) {
        aggregator.run(long_source, agg_idx_start, agg_idx_end, dp);
        if (dp.isInteger()) {
          if (long_values != null) {
            long_values[i] = dp.longValue();
          } else {
            double_values[i] = dp.toDouble();
          }
        } else {
          if (double_values == null) {
            double_values = new double[long_values.length];
            for (int x = 0; x < i; x++) {
              double_values[x] = long_values[x];
            }
          }
          
          double_values[i] = dp.toDouble();
        }
      } else {
        aggregator.run(double_source, agg_idx_start, agg_idx_end, 
            ((DownsampleConfig) node.config()).infectiousNan(), dp);
        if (dp.isInteger()) {
          if (long_values != null) {
            long_values[i] = dp.longValue();
          } else {
            double_values[i] = dp.toDouble();
          }
        } else {
          if (double_values == null) {
            double_values = new double[long_values.length];
            for (int x = 0; x < i; x++) {
              double_values[x] = long_values[x];
            }
          }
          
          double_values[i] = dp.toDouble();
        }
      }
    }
    
    has_next = false;
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

}

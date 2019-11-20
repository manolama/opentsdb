package net.opentsdb.query.egads;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;

public class ThresholdEvaluator {
  static final Logger LOG = LoggerFactory.getLogger(ThresholdEvaluator.class);
  
  public static final String UPPER = "upper";
  public static final String LOWER = "lower";
  
  private final double upper;
  private final boolean upper_is_scalar;
  private final double lower;
  private final boolean lower_is_scalar;
  private final boolean report_thresholds;
  private final TimeSeries current;
  private final QueryResult current_result;
  private final TimeSeries prediction;
  private final QueryResult prediction_result;
  
  private int idx;
  private double[] upper_thresholds;
  private double[] lower_thresholds;
  private List<AlertValue> alerts;
  
  public ThresholdEvaluator(final double upper, 
                            final boolean upper_is_scalar,
                            final double lower, 
                            final boolean lower_is_scalar, 
                            final int report_len,
                            final TimeSeries current,
                            final QueryResult current_result,
                            final TimeSeries prediction,
                            final QueryResult prediction_result) {
    this.upper = upper;
    this.upper_is_scalar = upper_is_scalar;
    this.lower = lower;
    this.lower_is_scalar = lower_is_scalar;
    if (report_len > 0) {
      this.report_thresholds = true;
      upper_thresholds = new double[report_len];
      lower_thresholds = new double[report_len];
    } else {
      this.report_thresholds = false;
    }
    this.current = current;
    this.current_result = current_result;
    this.prediction = prediction;
    this.prediction_result = prediction_result;
  }
  
  public boolean evaluate() {
    if (prediction == null) {
      // TODO - meh
      LOG.warn("Prediction was null.");
      return false;
    }
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
        prediction.iterator(NumericArrayType.TYPE);
    if (!pred_op.isPresent()) {
      LOG.warn("No array iterator for prediction.");
      return false;
    }
    
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = pred_op.get();
    if (!pred_it.hasNext()) {
      LOG.warn("No data in the prediction array.");
      return false;
    }
    
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) pred_it.next();
    if (value.value() == null) {
      LOG.warn("Null value?");
      return false;
    }
    
    TypeToken<? extends TimeSeriesDataType> current_type = null;
    for (final TypeToken<? extends TimeSeriesDataType> type : current.types()) {
      if (type == NumericType.TYPE ||
          type == NumericArrayType.TYPE ||
          type == NumericSummaryType.TYPE) {
        current_type = type;
        break;
      }
    }
    
    if (current_type == null) {
      LOG.warn("No type for current?");
      return false;
    }
    
    final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> current_op =
        current.iterator(current_type);
    if (!current_op.isPresent()) {
      LOG.warn("No data for type: " + current_type + " in current?");
      return false;
    }
    
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = 
        current_op.get();
    if (!iterator.hasNext()) {
      LOG.warn("No next type: " + current_type + " in current?");
      return false;
    }
    
    if (current_type == NumericType.TYPE) {
      runNumericType(iterator, value);
    } else if (current_type == NumericArrayType.TYPE) {
      runNumericArrayType(iterator, value);
    } else if (current_type == NumericSummaryType.TYPE) {
      runNumericSummaryType(iterator, value);
    } else {
      LOG.warn("Ummm, don't handle this type?");
      return false;
    }
    return true;
  }
  
  void runNumericType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      if (value.timestamp().compare(Op.LT, prediction_result.timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      AlertValue av = eval(value.timestamp(), value.value().toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
    }
  }
  
  void runNumericArrayType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    
    final TimeStamp ts = current_result.timeSpecification().start().getCopy();
    int wrote = 0;
    for (int i = value.value().offset(); i < value.value().end(); i++) {
      if (ts.compare(Op.LT, prediction_result.timeSpecification().start())) {
        ts.add(current_result.timeSpecification().interval());
        continue;
      }
      
      int idx = (int) ((ts.epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        ts.add(current_result.timeSpecification().interval());
        continue;
      }
      
      final AlertValue av = eval(ts, 
          (value.value().isInteger() ? (double) value.value().longArray()[i] :
            value.value().doubleArray()[i]), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
      
      ts.add(current_result.timeSpecification().interval());
      wrote++;
    }
    LOG.info("WROTE: " + wrote + " for " + current.id());
  }
  
  void runNumericSummaryType(
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator,
      final TimeSeriesValue<NumericArrayType> prediction) {
    final long prediction_base = prediction_result.timeSpecification().start().epoch();
    // TODO - won't work for biiiiig time ranges
    final long prediction_interval = prediction_result.timeSpecification()
        .interval().get(ChronoUnit.SECONDS);
    int summary = -1;
    
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (value.timestamp().compare(Op.LT, prediction_result.timeSpecification().start())) {
        continue;
      }
      
      int idx = (int) ((value.timestamp().epoch() - prediction_base) / prediction_interval);
      if (idx + prediction.value().offset() >= prediction.value().end()) {
        LOG.warn(idx + " beyond the prediction range.");
      }
      
      if (summary < 0) {
        summary = value.value().summariesAvailable().iterator().next();
      }
      AlertValue av = eval(value.timestamp(), value.value().value(summary).toDouble(), 
          (prediction.value().isInteger() ? (double) prediction.value().longArray()[idx] :
            prediction.value().doubleArray()[idx]));
      if (av != null) {
        if (alerts == null) {
          alerts = Lists.newArrayList();
        }
        alerts.add(av);
      }
    }
  }
  
  public AlertValue eval(final TimeStamp timestamp, 
                         final double current, 
                         final double prediction) {
    AlertValue result = null;
    if (upper != 0) {
      final double threshold;
      if (upper_is_scalar) {
        threshold = prediction + upper;
      } else {
        threshold = prediction + Math.abs((prediction * (upper / 100)));
      }
      if (upper_is_scalar && current > threshold) {
        result = AlertValue.newBuilder()
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER)
            .build();
      } else if (current > threshold) {
        result = AlertValue.newBuilder()
            .setDataPoint(current)
            .setMessage("** TEMP " + current + " is greater than " + threshold + " which is > than " + upper + "%")
            .setTimestamp(timestamp)
            .setThreshold(threshold)
            .setThresholdType(UPPER)
            .build();
      }
      
      if (report_thresholds) {
        if (idx >= upper_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many upper "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
                  + "properly in the ctor.");
        }
        upper_thresholds[idx] = prediction + upper;
      }
    }
    
    if (lower != 0) {
      final double threshold;
      if (lower_is_scalar) {
        threshold = prediction - lower;
      } else {
        threshold = prediction - Math.abs((prediction * (lower / (double) 100)));
      }
      if (lower_is_scalar && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is < " + threshold)
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER)
              .build();
        }
      } else if (current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setDataPoint(current)
              .setMessage("** TEMP " + current + " is less than " + threshold + " which is < than " + lower + "%")
              .setTimestamp(timestamp)
              .setThreshold(threshold)
              .setThresholdType(LOWER)
              .build();
        }
      }
      if (report_thresholds) {
        if (idx >= lower_thresholds.length) {
          throw new IllegalStateException("Attempted to write too many lower "
              + "thresholds [" + idx + "]. Make sure to set the report_len "
              + "properly in the ctor.");
        }
        lower_thresholds[idx] = prediction - lower;
      }
    }
    idx++;
    return result;
  }
  
  public double[] upperThresholds() {
    return upper_thresholds;
  }
  
  public double[] lowerThresholds() {
    return lower_thresholds;
  }
  
  public int index() {
    return idx;
  }
  
  public List<AlertValue> alerts() {
    return alerts;
  }
}

package net.opentsdb.query.egads;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.alert.AlertValue;

public class ThresholdEvaluator {

  private final double upper;
  private final boolean upper_is_scalar;
  private final double lower;
  private final boolean lower_is_scalar;
  private final boolean report_thresholds;
  
  private int idx;
  private double[] upper_thresholds;
  private double[] lower_thresholds;
  
  public ThresholdEvaluator(final double upper, 
                            final boolean upper_is_scalar,
                            final double lower, 
                            final boolean lower_is_scalar, 
                            final int report_len) {
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
        threshold = prediction + (prediction * (upper / 100));
      }
      if (upper_is_scalar && current > threshold) {
        result = AlertValue.newBuilder()
            .setDataPoint(current)
            .setMessage("TEMP " + current + " is > " + threshold)
            .setTimestamp(timestamp)
            .build();
      } else if (current > threshold) {
        result = AlertValue.newBuilder()
            .setDataPoint(current)
            .setMessage("TEMP " + current + " is greater than " + threshold + " which is > than " + upper)
            .setTimestamp(timestamp)
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
        threshold = prediction - (prediction * (lower / 100));
      }
      System.out.println("TH: " + threshold + "  C: " + current);
      if (lower_is_scalar && current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setDataPoint(current)
              .setMessage("TEMP " + current + " is < " + threshold)
              .setTimestamp(timestamp)
              .build();
        }
      } else if (current < threshold) {
        if (result == null) {
          result = AlertValue.newBuilder()
              .setDataPoint(current)
              .setMessage("TEMP " + current + " is less than " + threshold + " which is < than " + lower)
              .setTimestamp(timestamp)
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
  
}

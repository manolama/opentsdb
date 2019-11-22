package net.opentsdb.data.types.alert;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;

public class AlertValue implements AlertType, TimeSeriesValue<AlertType> {
  private State state;
  private TimeStamp timestamp;
  private MutableNumericType data_point;
  private String message;
  private MutableNumericType threshold;
  private String threshold_type;
  
  protected AlertValue(final Builder builder) {
    state = builder.state;
    timestamp = builder.timestamp;
    data_point = builder.data_point;
    message = builder.message;
    threshold = builder.threshold;
    threshold_type = builder.threshold_type;
  }
  
  @Override
  public State state() {
    return state;
  }
  
  @Override
  public String message() {
    return message;
  }

  @Override
  public NumericType dataPoint() {
    return data_point;
  }

  @Override
  public NumericType threshold() {
    return threshold;
  }
  
  @Override
  public String thresholdType() {
    return threshold_type;
  }
  
  @Override
  public TypeToken<AlertType> type() {
    return AlertType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public AlertType value() {
    return this;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private State state;
    private TimeStamp timestamp;
    private MutableNumericType data_point;
    private String message;
    private MutableNumericType threshold;
    private String threshold_type;
    
    public Builder setState(final State state) {
      this.state = state;
      return this;
    }
    
    public Builder setTimestamp(final TimeStamp timestamp) {
      this.timestamp = timestamp.getCopy();
      return this;
    }
    
    public Builder setDataPoint(final NumericType value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setDataPoint(final long value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setDataPoint(final double value) {
      data_point = new MutableNumericType(value);
      return this;
    }
    
    public Builder setMessage(final String message) {
      this.message = message;
      return this;
    }
    
    public Builder setThreshold(final long value) {
      threshold = new MutableNumericType(value);
      return this;
    }
    
    public Builder setThreshold(final double value) {
      threshold = new MutableNumericType(value);
      return this;
    }
    
    public Builder setThresholdType(final String threshold_type) {
      this.threshold_type = threshold_type;
      return this;
    }
    
    public AlertValue build() {
      return new AlertValue(this);
    }
  }
}

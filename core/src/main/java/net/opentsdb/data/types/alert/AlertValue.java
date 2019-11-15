package net.opentsdb.data.types.alert;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;

public class AlertValue implements AlertType, TimeSeriesValue<AlertType> {
  private TimeStamp timestamp;
  private MutableNumericType data_point;
  private String message;
  
  protected AlertValue(final Builder builder) {
    timestamp = builder.timestamp;
    data_point = builder.data_point;
    message = builder.message;
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
    private TimeStamp timestamp;
    private MutableNumericType data_point;
    private String message;
    
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
    
    public AlertValue build() {
      return new AlertValue(this);
    }
  }
}

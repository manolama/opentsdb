package net.opentsdb.data;

public interface LowLevelMetric extends LowLevelTimeSeries {
  static enum ValueFormat {
    INTEGER,
    FLOAT,
    DOUBLE
  }
  
  Format metricFormat();

  /** indices into the metric buffer. */
  int metricStart();
  int metricEnd();
  byte[] metricBuffer();
  
  /** We may have more values some day. */
  ValueFormat valueFormat();
  long intValue();
  float floatValue();
  double doubleValue();
  
  public interface HashedLowLevelMetric extends LowLevelMetric {
    long metricHash();
  }
}

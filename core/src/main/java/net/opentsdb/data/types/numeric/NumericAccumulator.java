package net.opentsdb.data.types.numeric;

public class NumericAccumulator {
  protected long[] long_values;
  protected double[] double_values;
  protected boolean longs = true;
  protected int value_idx = 0;
  protected MutableNumericValue dp;
  
  public NumericAccumulator() {
    long_values = new long[2];
    dp = new MutableNumericValue();
  }
  
  public MutableNumericValue dp() {
    return dp;
  }
  
  public void reset() {
    longs = true;
    value_idx = 0;
  }
  
  public int valueIndex() {
    return value_idx;
  }
  
  public void add(final long value) {
    if (!longs) {
      add((double) value);
      return;
    }
    if (value_idx >= long_values.length) {
      final long[] temp = new long[long_values.length < 1024 ? long_values.length * 2 : long_values.length + 1024];
      System.arraycopy(long_values, 0, temp, 0, long_values.length);
      long_values = temp;
    }
    long_values[value_idx] = value;
    value_idx++;
  }
  
  public void add(final double value) {
    if (longs) {
      shiftToDouble();
    }
    if (value_idx >= double_values.length) {
      final double[] temp = new double[double_values.length < 1024 ? double_values.length * 2 : double_values.length + 1024];
      System.arraycopy(double_values, 0, temp, 0, double_values.length);
      double_values = temp;
    }
    double_values[value_idx] = value;
    value_idx++;
  }
  
  private void shiftToDouble() {
    if (double_values == null) {
      double_values = new double[long_values.length];
    }
    if (value_idx >= double_values.length) {
      double_values = new double[long_values.length < 1024 ? long_values.length * 2 : long_values.length + 1024];
    }
    for (int i = 0; i < value_idx; i++) {
      double_values[i] = (double) long_values[i];
    }
    longs = false;
  }

  public void run(final NumericAggregator aggregator, final boolean infectious_nan) {
    if (longs) {
      aggregator.run(long_values, value_idx, dp);
    } else {
      aggregator.run(double_values, value_idx, infectious_nan, dp);
    }
  }
  
}

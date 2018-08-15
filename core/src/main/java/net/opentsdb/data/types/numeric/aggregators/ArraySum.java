package net.opentsdb.data.types.numeric.aggregators;

import java.util.Arrays;

public class ArraySum extends BaseArrayAggregator {

  @Override
  public void accumulate(final long[] values) {
    if (double_accumulator == null && long_accumulator == null) {
      long_accumulator = Arrays.copyOf(values, values.length);
      return;
    }
    
    if (long_accumulator != null) {
      for (int i = 0; i < values.length; i++) {
        long_accumulator[i] += values[i];
      }
    } else {
      for (int i = 0; i < values.length; i++) {
        double_accumulator[i] += values[i];
      }
    }
  }

  @Override
  public void accumulate(final double[] values, 
                         final boolean infectious_nans) {
    if (double_accumulator == null && long_accumulator == null) {
      double_accumulator = Arrays.copyOf(values, values.length);
      return;
    }
    
    if (double_accumulator == null) {
      double_accumulator = new double[long_accumulator.length];
      for (int i = 0; i < long_accumulator.length; i++) {
        double_accumulator[i] = long_accumulator[i];
      }
      long_accumulator = null;
    }
    for (int i = 0; i < values.length; i++) {
      double_accumulator[i] += values[i];
    }
  }

  @Override
  public String id() {
    return "sum";
  }

}

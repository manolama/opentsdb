// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric.aggregators;

import java.util.Arrays;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.AggregatorConfig;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.PooledObject;

/**
 * Computes the average across the array. Returns a double array always.
 * 
 * @since 3.0
 */
public class ArrayAverageFactory extends BaseArrayFactory {

  public static final String TYPE = "Avg";
  
  private ArrayObjectPool long_pool;
  private ArrayObjectPool double_pool;
  private ArrayObjectPool int_pool;
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public NumericArrayAggregator newAggregator() {
    return new ArrayAverage(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final AggregatorConfig config) {
    if (config != null && config instanceof NumericAggregatorConfig) {
      return new ArrayAverage(
          ((NumericAggregatorConfig) config).infectiousNan(), this);
    }
    return new ArrayAverage(false, this);
  }
  
  @Override
  public NumericArrayAggregator newAggregator(final boolean infectious_nan) {
    return new ArrayAverage(infectious_nan, this);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    tsdb.getRegistry().registerPlugin(NumericArrayAggregatorFactory.class, 
        "average", this);
    long_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(LongArrayPool.TYPE);
    double_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(DoubleArrayPool.TYPE);
    int_pool = (ArrayObjectPool) tsdb.getRegistry().getObjectPool(IntArrayPool.TYPE);
    return Deferred.fromResult(null);
  }
  
  /** Getters for UTs. */
  ArrayObjectPool longPool() {
    return long_pool;
  }
  
  ArrayObjectPool doublePool() {
    return double_pool;
  }
  
  ArrayObjectPool intPool() {
    return int_pool;
  }
  
  public static class ArrayAverage extends BaseArrayAggregator {

    protected final ArrayAverageFactory factory;
    protected int[] counts;
    protected PooledObject int_pooled;
    protected double[] results;
    protected int end;
    
    public ArrayAverage(final boolean infectious_nans,
                        final ArrayAverageFactory factory) {
      super(infectious_nans);
      this.factory = factory;
    }

    @Override
    public void accumulate(final long[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        if (factory.longPool() != null) {
          pooled = factory.longPool().claim(to - from);
          if (factory.intPool() != null) {
            int_pooled = factory.intPool().claim(to - from);
            counts = (int[]) int_pooled.object();
          } else {
            counts = new int[to - from];
          }
          
          long_accumulator = (long[]) pooled.object();
          System.arraycopy(values, from, long_accumulator, 0, to - from);
        } else {
          long_accumulator = Arrays.copyOfRange(values, from, to);
          counts = new int[to - from];
        }
        end = to - from;
        Arrays.fill(counts, 0, end, 1);
        System.out.println("      " + Arrays.toString(long_accumulator));
        return;
      }
      
      if (long_accumulator != null) {
        if (to - from != end) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + long_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          counts[idx]++;
          long_accumulator[idx++] += values[i];
        }
      } else {
        if (to - from != double_accumulator.length) {
          throw new IllegalArgumentException("Values of length " 
              + (to - from) + " did not match the original lengh of " 
              + double_accumulator.length);
        }
        int idx = 0;
        for (int i = from; i < to; i++) {
          counts[idx]++;
          double_accumulator[idx++] += values[i];
        }
      }
      
      System.out.println("      " + Arrays.toString(long_accumulator));
    }

    @Override
    public void accumulate(final double value, final int idx) {
      if (double_accumulator == null) {
        throw new IllegalStateException("The accumulator has not been initialized.");
      }
      if (Double.isNaN(value)) {
        if (infectious_nans && !Double.isNaN(double_accumulator[idx])) {
          double_accumulator[idx] = Double.NaN;
          counts[idx] = 0;
        }
      } else {
        if (!infectious_nans || !Double.isNaN(double_accumulator[idx])) {
          if (Double.isNaN(double_accumulator[idx])) {
            double_accumulator[idx] = value;
          } else {
            double_accumulator[idx] += value;
          }
          counts[idx]++;
        }
      }
    }

    @Override
    public void accumulate(final double[] values, 
                           final int from, 
                           final int to) {
      if (double_accumulator == null && long_accumulator == null) {
        if (factory.doublePool() != null) {
          pooled = factory.doublePool().claim(to - from);
          if (factory.intPool() != null) {
            int_pooled = factory.intPool().claim(to - from);
            counts = (int[]) int_pooled.object();
          } else {
            counts = new int[to - from];
          }
          
          double_accumulator = (double[]) pooled.object();
          System.arraycopy(double_accumulator, 0, values, from, to - from);
        } else {
          double_accumulator = Arrays.copyOfRange(values, from, to);
          counts = new int[double_accumulator.length];
        }
        for (int i = 0; i < double_accumulator.length; i++) {
          if(!Double.isNaN(double_accumulator[i])){
            counts[i] = 1;
          }
        }
        return;
      }
      
      if (double_accumulator == null) {
        PooledObject double_pooled = null;
        if (factory.doublePool() != null) {
          double_pooled = factory.doublePool().claim(long_accumulator.length);
          double_accumulator = (double[]) double_pooled.object();
        } else {
          double_accumulator = new double[long_accumulator.length];
        }
        for (int i = 0; i < long_accumulator.length; i++) {
          double_accumulator[i] = long_accumulator[i];
        }
        long_accumulator = null;
        
        if (double_pooled != null) {
          pooled.release();
          pooled = double_pooled;
        }
      }
      
      if (to - from != double_accumulator.length) {
        throw new IllegalArgumentException("Values of length " 
            + (to - from) + " did not match the original lengh of " 
            + double_accumulator.length);
      }
      
      int idx = 0;
      for (int i = from; i < to; i++) {
        accumulate(values[i], idx);
        idx++;
      }
    }

    @Override
    public void combine(final NumericArrayAggregator aggregator) {
      ArrayAverage arrayAverage = (ArrayAverage) aggregator;
      double[] double_accumulator = arrayAverage.double_accumulator;
      long[] long_accumulator = arrayAverage.long_accumulator;
      int[] counts = arrayAverage.counts;

      if (double_accumulator != null) {
        combine(double_accumulator, counts);
      }
      if (long_accumulator != null) {
        combine(long_accumulator, counts);
      }
    }

    private void combine(final long[] values, final int[] counts) {
      if (this.long_accumulator == null) {
        if (factory.longPool() != null) {
          pooled = factory.longPool().claim(values.length);
          if (factory.intPool() != null) {
            int_pooled = factory.intPool().claim(counts.length);
            this.counts = (int[]) int_pooled.object();
          } else {
            this.counts = new int[counts.length];
          }
          
          long_accumulator = (long[]) pooled.object();
          System.arraycopy(long_accumulator, 0, values, 0, values.length);
        } else {
          this.long_accumulator = Arrays.copyOfRange(values, 0, values.length);
          this.counts = Arrays.copyOfRange(counts, 0, counts.length);
        }
      } else {
        for (int i = 0; i < values.length; i++) {
          this.long_accumulator[i] += values[i];
          this.counts[i] += counts[i];
        }
      }
    }

    private void combine(final double[] values, final int[] counts) {
      if (this.double_accumulator == null) {
        if (factory.doublePool() != null) {
          pooled = factory.doublePool().claim(values.length);
          if (factory.intPool() != null) {
            int_pooled = factory.intPool().claim(counts.length);
            this.counts = (int[]) int_pooled.object();
          } else {
            this.counts = new int[counts.length];
          }
          
          double_accumulator = (double[]) pooled.object();
          System.arraycopy(double_accumulator, 0, values, 0, values.length);
        } else {
          this.double_accumulator = Arrays.copyOfRange(values, 0, values.length);
          this.counts = Arrays.copyOfRange(counts, 0, counts.length);
        }
      } else {
        for (int i = 0; i < values.length; i++) {
          double value = values[i];
          if (Double.isNaN(value)) {
            if (infectious_nans && !Double.isNaN(this.double_accumulator[i])) {
              this.double_accumulator[i] = Double.NaN;
              this.counts[i] = 0;
            }
          } else {
            if (!infectious_nans || !Double.isNaN(double_accumulator[i])) {
              if (Double.isNaN(double_accumulator[i])) {
                double_accumulator[i] = value;
              } else {
                double_accumulator[i] += value;
              }
              this.counts[i] += counts[i];
            }
          }
        }
      }
    }

    @Override
    public boolean isInteger() {
      return false;
    }
    
    @Override
    public long[] longArray() {
      return null;
    }
    
    @Override
    public double[] doubleArray() {
      if (results == null) {
        results = new double[counts.length];
        for (int i = 0; i < counts.length; i++) {
          // zero / zero will give us NaN.
          if (long_accumulator != null) {
            results[i] = (double) long_accumulator[i] / (double) counts[i];
          } else {
            results[i] = double_accumulator[i] / (double) counts[i];
          }
        }
        // allow the other arrays to be free.
        long_accumulator = null;
        double_accumulator = null;
      }
      return results;
    }
    
    @Override
    public int end() {
      return end;
    }
    
    @Override
    public String name() {
      return ArrayAverageFactory.TYPE;
    }
    
    @Override
    public void close() {
      super.close();
      if (int_pooled != null) {
        int_pooled.release();
      }
    }
  }
}

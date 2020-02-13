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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.IntArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.MockArrayObjectPool;

public class TestArrayAverage {
  
  private static ArrayAverageFactory NON_POOLED;
  private static ArrayAverageFactory POOLED;
  private static ArrayObjectPool INT_POOL;
  private static ArrayObjectPool LONG_POOL;
  private static ArrayObjectPool DOUBLE_POOL;
  
  @BeforeClass
  public static void beforeClass() {
    NON_POOLED = mock(ArrayAverageFactory.class);
    POOLED = mock(ArrayAverageFactory.class);
    INT_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new IntArrayPool())
        .setInitialCount(4)
        .setArrayLength(5)
        .setId(IntArrayPool.TYPE)
        .build());
    LONG_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new LongArrayPool())
        .setInitialCount(4)
        .setArrayLength(5)
        .setId(LongArrayPool.TYPE)
        .build());
    DOUBLE_POOL = new MockArrayObjectPool(DefaultObjectPoolConfig.newBuilder()
        .setAllocator(new DoubleArrayPool())
        .setInitialCount(4)
        .setArrayLength(5)
        .setId(DoubleArrayPool.TYPE)
        .build());
    when(POOLED.intPool()).thenReturn(INT_POOL);
    when(POOLED.longPool()).thenReturn(LONG_POOL);
    when(POOLED.doublePool()).thenReturn(DOUBLE_POOL);
  }
  
  @Test
  public void longs() {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertArrayEquals(new double[] { }, agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, NON_POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void longsPooled() {
    ArrayAverageFactory.ArrayAverage agg = 
        new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    System.out.println(Arrays.toString(agg.doubleArray()));
    assertPooledArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(0, agg.end());
    assertPooledArrayEquals(new double[] { }, agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false, POOLED);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doubles() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -24, 2.5, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArrayAverageFactory.ArrayAverage(true);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, Double.NaN, 2.5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mixed() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(4, agg.end());
    assertArrayEquals(new double[] { 22.5, -18.5, 2.5, 0 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void offsets() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new long[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
    
    agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 }, 1, 3);
    agg.accumulate(new double[] { 3, -13, 5, -1 }, 1, 3);
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertEquals(0, agg.offset());
    assertEquals(2, agg.end());
    assertArrayEquals(new double[] { -18.5, 2.5 }, agg.doubleArray(), 0.001);
  }

  @Test
  public void singleNanDouble() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    assertArrayEquals(new double[] { 3, Double.NaN, 5, -1 }, agg.doubleArray(), 0.001);
  }
  
  @Test
  public void nanThenReals() throws Exception {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    agg.accumulate(new double[] { 5, 2, Double.NaN, 2 });
    assertArrayEquals(new double[] { 4, 2, 5, 0.5 }, agg.doubleArray(), 0.001);
  }

  @Test
  public void combine() {
    ArrayAverageFactory.ArrayAverage agg1 = new ArrayAverageFactory.ArrayAverage(false);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayAverageFactory.ArrayAverage agg2 = new ArrayAverageFactory.ArrayAverage(false);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayAverageFactory.ArrayAverage combiner = new ArrayAverageFactory.ArrayAverage(false);
    combiner.combine(agg1);
    assertArrayEquals(new double[] {7, 5, 9, -20}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 1, 1, 2}, combiner.counts);

    combiner.combine(agg2);
    assertArrayEquals(new double[] {10, 13, 9, -18}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {3, 2, 1, 3}, combiner.counts);

    assertArrayEquals(new double[] {3.3333333333333335, 6.5, 9, -6}, combiner.doubleArray(), 0.0001);
  }

  @Test
  public void combineWithInfectiousNan() {
    ArrayAverageFactory.ArrayAverage agg1 = new ArrayAverageFactory.ArrayAverage(true);
    agg1.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    agg1.accumulate(new double[] { 4, 5, Double.NaN, -19 });

    ArrayAverageFactory.ArrayAverage agg2 = new ArrayAverageFactory.ArrayAverage(true);
    agg2.accumulate(new double[] { 3, 8, Double.NaN, 2 });

    ArrayAverageFactory.ArrayAverage combiner = new ArrayAverageFactory.ArrayAverage(true);
    combiner.combine(agg1);
    assertArrayEquals(new double[] {7, Double.NaN, Double.NaN, -20}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 0, 2}, combiner.counts);

    combiner.combine(agg2);
    assertArrayEquals(new double[] {10, Double.NaN, Double.NaN, -18}, combiner.double_accumulator, 0.001);
    assertArrayEquals(new int[] {3, 0, 0, 3}, combiner.counts);

    assertArrayEquals(new double[] {3.3333333333333335, Double.NaN, Double.NaN, -6}, combiner.doubleArray(), 0.0001);
  }

  @Test
  public void accumulate() {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(false);
    agg.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    assertArrayEquals(new double[] {3, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {1, 0, 1, 1}, agg.counts);

    agg.accumulate(1.0, 0);
    assertArrayEquals(new double[] {4, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 1, 1}, agg.counts);

    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new double[] {4, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 1, 1}, agg.counts);

    agg.accumulate(2.0, 1);
    assertArrayEquals(new double[] {4, 2, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 1, 1, 1}, agg.counts);

    agg.accumulate(Double.NaN, 2);
    assertArrayEquals(new double[] {4, 2, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 1, 1, 1}, agg.counts);
  }

  @Test
  public void accumulateWithInfectiousNan() {
    ArrayAverageFactory.ArrayAverage agg = new ArrayAverageFactory.ArrayAverage(true);

    agg.accumulate(new double[] { 3, Double.NaN, 9, -1 });
    assertArrayEquals(new double[] {3, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {1, 0, 1, 1}, agg.counts);

    agg.accumulate(1.0, 0);
    assertArrayEquals(new double[] {4, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 1, 1}, agg.counts);

    agg.accumulate(Double.NaN, 1);
    assertArrayEquals(new double[] {4, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 1, 1}, agg.counts);

    agg.accumulate(2.0, 1);
    assertArrayEquals(new double[] {4, Double.NaN, 9, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 1, 1}, agg.counts);

    agg.accumulate(Double.NaN, 2);
    assertArrayEquals(new double[] {4, Double.NaN, Double.NaN, -1 }, agg.double_accumulator, 0.001);
    assertArrayEquals(new int[] {2, 0, 0, 1}, agg.counts);
  }

  public static void assertPooledArrayEquals(final double[] expected, 
                                             final double[] tested,
                                             final double epsilon) {
    final double[] copy = Arrays.copyOf(tested, expected.length);
    assertArrayEquals(expected, copy, epsilon);
  }
  
}

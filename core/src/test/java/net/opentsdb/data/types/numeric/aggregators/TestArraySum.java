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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class TestArraySum {

  @Test
  public void longs() {
    ArraySum agg = new ArraySum(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new long[] { 3, -13, 5, -1 });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertArrayEquals(new long[] { 45, -37, 5, 0 }, agg.longArray());
    
    agg = new ArraySum(false);
    agg.accumulate(new long[] { });
    agg.accumulate(new long[] { });
    
    assertTrue(agg.isInteger());
    assertNull(agg.doubleArray());
    assertArrayEquals(new long[] { }, agg.longArray());
    
    // bad length
    try {
      agg.accumulate(new long[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void doubles() throws Exception {
    ArraySum agg = new ArraySum(false);
    agg.accumulate(new double[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    
    // non-infectious nans
    agg = new ArraySum(false);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 45, -24, 5, -1 }, agg.doubleArray(), 0.001);
    
    // infectious nans
    agg = new ArraySum(true);
    agg.accumulate(new double[] { 42, -24, 0, Double.NaN });
    agg.accumulate(new double[] { 3, Double.NaN, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 45, Double.NaN, 5, Double.NaN }, 
        agg.doubleArray(), 0.001);
    
    // bad length
    try {
      agg.accumulate(new double[] { 1 });
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void mixed() throws Exception {
    ArraySum agg = new ArraySum(false);
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
    
    agg = new ArraySum(false);
    agg.accumulate(new double[] { 3, -13, 5, -1 });
    agg.accumulate(new long[] { 42, -24, 0, 1 });
    
    assertFalse(agg.isInteger());
    assertNull(agg.longArray());
    assertArrayEquals(new double[] { 45, -37, 5, 0 }, agg.doubleArray(), 0.001);
  }
}
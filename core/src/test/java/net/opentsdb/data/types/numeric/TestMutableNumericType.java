// This file is part of OpenTSDB.
// Copyright (C) 2014-2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;

/**
 * Tests {@link MutableNumericType}.
 */
public class TestMutableNumericType {
  private MillisecondTimeStamp ts;
  
  @Before
  public void before() {
    ts = new MillisecondTimeStamp(1);
  }
  
  @Test
  public void ctors() throws Exception {
    MutableNumericType dp = new MutableNumericType(ts, 42);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    dp = new MutableNumericType(ts, 42.5);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    dp = new MutableNumericType(dp);
    assertNotSame(ts, dp.timestamp());
    assertEquals(1, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(42.5, dp.doubleValue(), 0.001);
    
    try {
      new MutableNumericType(null, 42);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType(null, 42.5);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new MutableNumericType((TimeSeriesValue<NumericType>) null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void reset() throws Exception {
    final MutableNumericType dp = new MutableNumericType(ts, 42.5);
    
    TimeStamp ts2 = new MillisecondTimeStamp(2);
    dp.reset(ts2, 42);
    assertNotSame(ts, dp.timestamp());
    assertEquals(2, dp.timestamp().msEpoch());
    assertTrue(dp.isInteger());
    assertEquals(42, dp.longValue());
    
    ts2 = new MillisecondTimeStamp(3);
    dp.reset(ts2, 24.5);
    assertNotSame(ts, dp.timestamp());
    assertEquals(3, dp.timestamp().msEpoch());
    assertFalse(dp.isInteger());
    assertEquals(24.5, dp.doubleValue(), 0.001);
    
    final MutableNumericType dupe = new MutableNumericType();
    assertNotSame(ts, dupe.timestamp());
    assertEquals(0, dupe.timestamp().msEpoch());
    assertTrue(dupe.isInteger());
    assertEquals(0, dupe.longValue());
    
    dupe.reset(dp);
    assertNotSame(ts, dupe.timestamp());
    assertEquals(3, dupe.timestamp().msEpoch());
    assertFalse(dupe.isInteger());
    assertEquals(24.5, dupe.doubleValue(), 0.001);
  }

}
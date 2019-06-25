// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.data.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.temporal.ChronoUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.ZonedNanoTimeStamp;

public class TestNumericLongArrayType {
  private static final int BASE_TIME = 1356998400;
  
  private static MockNumericLongArrayTimeSeries PTS;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    ZonedNanoTimeStamp zts_a = new ZonedNanoTimeStamp(BASE_TIME + (60L * 10L), 55, Const.UTC);
    ZonedNanoTimeStamp zts_b = new ZonedNanoTimeStamp(BASE_TIME + (60L * 25L), 801, Const.UTC);
    PTS = new MockNumericLongArrayTimeSeries(mock(PartialTimeSeriesSet.class), 42);
    PTS.addValue(BASE_TIME, 4)
       .addValue((((long) BASE_TIME + (60L * 5L)) * 1000L) + 250L, 8)
       .addValue(zts_a, -16)
       .addValue(BASE_TIME + 60 * 15, 32.90)
       .addValue((((long) BASE_TIME + (60L * 20L)) * 1000L) + 750L, 64.85)
       .addValue(zts_b, -128.75);
  }
  
  @Test
  public void timestampUnits() throws Exception {
    assertEquals(ChronoUnit.SECONDS, NumericLongArrayType.timestampUnits(PTS.data(), 0));
    assertEquals(ChronoUnit.MILLIS, NumericLongArrayType.timestampUnits(PTS.data(), 2));
    assertEquals(ChronoUnit.NANOS, NumericLongArrayType.timestampUnits(PTS.data(), 4));
    assertEquals(ChronoUnit.SECONDS, NumericLongArrayType.timestampUnits(PTS.data(), 7));
    assertEquals(ChronoUnit.MILLIS, NumericLongArrayType.timestampUnits(PTS.data(), 9));
    assertEquals(ChronoUnit.NANOS, NumericLongArrayType.timestampUnits(PTS.data(), 11));
  }
  
  @Test
  public void timestampInMillis() throws Exception {
    assertEquals(BASE_TIME * 1000L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 0));
    assertEquals((((long) BASE_TIME + (60L * 5L)) * 1000L) + 250L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 2));
    assertEquals((BASE_TIME + (60L * 10L)) * 1000L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 4));
    assertEquals((BASE_TIME + (60L * 15L)) * 1000L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 7));
    assertEquals((((long) BASE_TIME + (60L * 20L)) * 1000L) + 750L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 9));
    assertEquals((BASE_TIME + (60L * 25L)) * 1000L, 
        NumericLongArrayType.timestampInMillis(PTS.data(), 11));
  }
  
  @Test
  public void timestampInNanos() throws Exception {
    ZonedNanoTimeStamp ts = new ZonedNanoTimeStamp(0, 0, Const.UTC);
    NumericLongArrayType.timestampInNanos(PTS.data(), 0, ts);
    assertEquals(BASE_TIME, ts.epoch());
    assertEquals(0, ts.nanos());
    
    NumericLongArrayType.timestampInNanos(PTS.data(), 2, ts);
    assertEquals(BASE_TIME + (60L * 5L), ts.epoch());
    assertEquals(250L * 1000000, ts.nanos());
    
    NumericLongArrayType.timestampInNanos(PTS.data(), 4, ts);
    assertEquals(BASE_TIME + (60L * 10L), ts.epoch());
    assertEquals(55, ts.nanos());
    
    NumericLongArrayType.timestampInNanos(PTS.data(), 7, ts);
    assertEquals(BASE_TIME + (60L * 15L), ts.epoch());
    assertEquals(0, ts.nanos());
    
    NumericLongArrayType.timestampInNanos(PTS.data(), 9, ts);
    assertEquals(BASE_TIME + (60L * 20L), ts.epoch());
    assertEquals(750L * 1000000, ts.nanos());
    
    NumericLongArrayType.timestampInNanos(PTS.data(), 11, ts);
    assertEquals(BASE_TIME + (60L * 25L), ts.epoch());
    assertEquals(801, ts.nanos());
  }

  @Test
  public void isDouble() throws Exception {
    assertFalse(NumericLongArrayType.isDouble(PTS.data(), 0));
    assertFalse(NumericLongArrayType.isDouble(PTS.data(), 2));
    assertFalse(NumericLongArrayType.isDouble(PTS.data(), 4));
    assertTrue(NumericLongArrayType.isDouble(PTS.data(), 7));
    assertTrue(NumericLongArrayType.isDouble(PTS.data(), 9));
    assertTrue(NumericLongArrayType.isDouble(PTS.data(), 11));
  }
  
  @Test
  public void getDouble() throws Exception {
    assertEquals(32.90, NumericLongArrayType.getDouble(PTS.data(), 7), 0.001);
    assertEquals(64.85, NumericLongArrayType.getDouble(PTS.data(), 9), 0.001);
    assertEquals(-128.75, NumericLongArrayType.getDouble(PTS.data(), 11), 0.001);
  }
}

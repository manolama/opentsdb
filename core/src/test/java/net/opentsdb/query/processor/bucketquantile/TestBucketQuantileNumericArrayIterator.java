// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericArrayType;

public class TestBucketQuantileNumericArrayIterator {

  @Test
  public void hasData() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1608076800);
    double[] results = new double[] { 42, 24, 0 };
    TimeSeriesStringId base_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build();
    BucketQuantileNumericArrayIterator iterator = 
        new BucketQuantileNumericArrayIterator(ts, results, 3, base_id, "quantiles", 0.999);
    assertEquals(NumericArrayType.TYPE, iterator.getType());
    assertEquals(NumericArrayType.TYPE, iterator.type());
    assertTrue(iterator.hasNext());
    assertSame(ts, iterator.timestamp());
    assertSame(iterator, iterator.next());
    assertFalse(iterator.hasNext());
    assertEquals(0, iterator.offset());
    assertEquals(3, iterator.end());
    assertFalse(iterator.isInteger());
    assertNull(iterator.longArray());
    assertSame(results, iterator.doubleArray());
    assertSame(iterator, iterator.value());
    TimeSeriesStringId id = (TimeSeriesStringId) iterator.id();
    assertEquals("quantiles", id.metric());
    assertEquals(3, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("DEN", id.tags().get("dc"));
    assertEquals("99.9", id.tags().get("_percentile"));
  }
  
  @Test
  public void noData() throws Exception {
    TimeStamp ts = new SecondTimeStamp(1608076800);
    double[] results = new double[] { 42, 24, 0 };
    TimeSeriesStringId base_id = BaseTimeSeriesStringId.newBuilder()
        .setMetric("m_0_250")
        .setTags(ImmutableMap.of("host", "web01", "dc", "DEN"))
        .build();
    BucketQuantileNumericArrayIterator iterator = 
        new BucketQuantileNumericArrayIterator(ts, results, 0, base_id, "quantiles", 0.999);
    assertEquals(NumericArrayType.TYPE, iterator.getType());
    assertEquals(NumericArrayType.TYPE, iterator.type());
    assertFalse(iterator.hasNext());
    assertSame(ts, iterator.timestamp());
    assertEquals(0, iterator.offset());
    assertEquals(0, iterator.end());
    assertFalse(iterator.isInteger());
    assertNull(iterator.longArray());
    assertSame(results, iterator.doubleArray());
    assertSame(iterator, iterator.value());
    TimeSeriesStringId id = (TimeSeriesStringId) iterator.id();
    assertEquals("quantiles", id.metric());
    assertEquals(3, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertEquals("DEN", id.tags().get("dc"));
    assertEquals("99.9", id.tags().get("_percentile"));
  }
  
}

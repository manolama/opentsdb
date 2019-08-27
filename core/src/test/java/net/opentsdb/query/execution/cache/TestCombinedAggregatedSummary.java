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
package net.opentsdb.query.execution.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;
import com.google.common.collect.Lists;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

public class TestCombinedAggregatedSummary {
  private static final int BASE_TIME = 1546300800;
  
  @Test
  public void noGaps() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME, false);
    CombinedAggregatedSummary iterator = new CombinedAggregatedSummary(result, rs);
    
    int ts = BASE_TIME;
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(ts, v.timestamp().epoch());
    assertEquals(7, v.value().summariesAvailable().size());
    assertEquals(180, v.value().value(0).longValue());
    assertEquals(35, v.value().value(1).longValue());
    assertEquals(3, v.value().value(2).longValue());
    assertEquals(15, v.value().value(3).longValue());
    assertEquals(7.825, v.value().value(5).doubleValue(), 0.001);
    assertEquals(8, v.value().value(6).longValue());
    assertEquals(13, v.value().value(7).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapAtStart() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME + 3600, false);
    CombinedAggregatedSummary iterator = new CombinedAggregatedSummary(result, rs);
    
    int ts = BASE_TIME + 3600;
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(ts, v.timestamp().epoch());
    assertEquals(7, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(0).longValue());
    assertEquals(29, v.value().value(1).longValue());
    assertEquals(3, v.value().value(2).longValue());
    assertEquals(15, v.value().value(3).longValue());
    assertEquals(7.25, v.value().value(5).doubleValue(), 0.001);
    assertEquals(5, v.value().value(6).longValue());
    assertEquals(13, v.value().value(7).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapInMiddle() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME, true);
    CombinedAggregatedSummary iterator = new CombinedAggregatedSummary(result, rs);
    
    int ts = BASE_TIME;
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(ts, v.timestamp().epoch());
    assertEquals(7, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(0).longValue());
    assertEquals(17, v.value().value(1).longValue());
    assertEquals(5, v.value().value(2).longValue());
    assertEquals(15, v.value().value(3).longValue());
    assertEquals(8.4, v.value().value(5).doubleValue(), 0.001);
    assertEquals(8, v.value().value(6).longValue());
    assertEquals(13, v.value().value(7).longValue());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void gapAtEnd() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME, false);
    rs.remove(2);
    CombinedAggregatedSummary iterator = new CombinedAggregatedSummary(result, rs);
    
    int ts = BASE_TIME;
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
    assertEquals(ts, v.timestamp().epoch());
    assertEquals(7, v.value().summariesAvailable().size());
    assertEquals(120, v.value().value(0).longValue());
    assertEquals(24, v.value().value(1).longValue());
    assertEquals(3, v.value().value(2).longValue());
    assertEquals(10, v.value().value(3).longValue());
    assertEquals(6.05, v.value().value(5).doubleValue(), 0.001);
    assertEquals(8, v.value().value(6).longValue());
    assertEquals(4, v.value().value(7).longValue());
    assertFalse(iterator.hasNext());
  }
  
  List<Pair<QueryResult, TimeSeries>> generateSeries(final int num_results, 
                                                     final int start_timestamp,
                                                     final boolean gaps) {
    List<Pair<QueryResult, TimeSeries>> results = Lists.newArrayList();
    int timestamp = BASE_TIME;
    int value = 60;

    QueryResult result = mock(QueryResult.class);
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    MutableNumericSummaryValue summary = MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(timestamp))
        .addValue(0, value)
        .addValue(1, 6)
        .addValue(2, 5)
        .addValue(3, 10)
        .addValue(5, 7.2)
        .addValue(6, 8)
        .addValue(7, 5)
        .build();
    ts.addValue(summary);
    if (start_timestamp <= timestamp) {
      results.add(new Pair<>(result, ts));
    }
    timestamp += 3600;
    
    // next
    result = mock(QueryResult.class);
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    summary = MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(timestamp))
        .addValue(0, value)
        .addValue(1, 18)
        .addValue(2, 3)
        .addValue(3, 9)
        .addValue(5, 4.9)
        .addValue(6, 5)
        .addValue(7, 4)
        .build();
    ts.addValue(summary);
    if (num_results >= 2 && start_timestamp <= timestamp && !gaps) {
      results.add(new Pair<>(result, ts));
    }
    timestamp += 3600;
    
    // next
    result = mock(QueryResult.class);
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    summary = MutableNumericSummaryValue.newBuilder()
        .setTimeStamp(new SecondTimeStamp(timestamp))
        .addValue(0, value)
        .addValue(1, 11)
        .addValue(2, 6)
        .addValue(3, 15)
        .addValue(5, 9.6)
        .addValue(6, 8)
        .addValue(7, 13)
        .build();
    ts.addValue(summary);
    if (num_results >= 3 && start_timestamp <= timestamp) {
      results.add(new Pair<>(result, ts));
    }
    timestamp += 3600;
    return results;
  }
}

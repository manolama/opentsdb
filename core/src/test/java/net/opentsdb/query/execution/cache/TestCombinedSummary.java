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

public class TestCombinedSummary {
  private static final int BASE_TIME = 1546300800;

  @Test
  public void noGaps() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME, false);
    CombinedSummary iterator = new CombinedSummary(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want++, v.value().value(0).longValue());
      assertEquals(1, v.value().value(1).longValue());
      ts += 3600;
    }
    assertEquals(BASE_TIME + (86400 * 3), ts);
  }
  
  @Test
  public void gapAtStart() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME + 86400, false);
    CombinedSummary iterator = new CombinedSummary(result, rs);
    
    int ts = BASE_TIME + 86400;
    int want = 24;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want++, v.value().value(0).longValue());
      assertEquals(1, v.value().value(1).longValue());
      ts += 3600;
    }
    assertEquals(BASE_TIME + (86400 * 3), ts);
  }
  
  @Test
  public void gapAtEnd() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(2, BASE_TIME, false);
    CombinedSummary iterator = new CombinedSummary(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want++, v.value().value(0).longValue());
      assertEquals(1, v.value().value(1).longValue());
      ts += 3600;
    }
    assertEquals(BASE_TIME + (86400 * 2), ts);
  }
  
  @Test
  public void gapInMiddle() throws Exception {
    CombinedResult result = mock(CombinedResult.class);
    List<Pair<QueryResult, TimeSeries>> rs = generateSeries(3, BASE_TIME, true);
    CombinedSummary iterator = new CombinedSummary(result, rs);
    
    int ts = BASE_TIME;
    int want = 0;
    while (iterator.hasNext()) {
      TimeSeriesValue<NumericSummaryType> v = (TimeSeriesValue<NumericSummaryType>) iterator.next();
      assertEquals(ts, v.timestamp().epoch());
      assertEquals(want++, v.value().value(0).longValue());
      assertEquals(1, v.value().value(1).longValue());
      ts += 3600;
      if (ts == BASE_TIME + (86400)) {
        ts = BASE_TIME + (86400 * 2);
        want += 24;
      }
    }
    assertEquals(BASE_TIME + (86400 * 3), ts);
  }
  
  List<Pair<QueryResult, TimeSeries>> generateSeries(final int num_results, 
                                                     final int start_timestamp,
                                                     final boolean gaps) {
    List<Pair<QueryResult, TimeSeries>> results = Lists.newArrayList();
    int timestamp = BASE_TIME;
    int value = 0;

    QueryResult result = mock(QueryResult.class);
    MockTimeSeries ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    if (start_timestamp <= timestamp) {
      for (int i = 0; i < 24; i++) {
        MutableNumericSummaryValue summary = MutableNumericSummaryValue.newBuilder()
            .setTimeStamp(new SecondTimeStamp(timestamp))
            .addValue(0, value++)
            .addValue(1, 1)
            .build();
        ts.addValue(summary);
        timestamp += 3600;
      }

      results.add(new Pair<>(result, ts));
    } else {
      timestamp += 86400;
      value += 24;
    }
    
    // next
    result = mock(QueryResult.class);
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    if (num_results >= 2 && start_timestamp <= timestamp && !gaps) {
      for (int i = 0; i < 24; i++) {
        MutableNumericSummaryValue summary = MutableNumericSummaryValue.newBuilder()
            .setTimeStamp(new SecondTimeStamp(timestamp))
            .addValue(0, value++)
            .addValue(1, 1)
            .build();
        ts.addValue(summary);
        timestamp += 3600;
      }

      results.add(new Pair<>(result, ts));
    } else {
      timestamp += 86400;
      value += 24;
    }
    
    // next
    result = mock(QueryResult.class);
    ts = new MockTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .build());
    if (num_results >= 3 && start_timestamp <= timestamp) {
      for (int i = 0; i < 24; i++) {
        MutableNumericSummaryValue summary = MutableNumericSummaryValue.newBuilder()
            .setTimeStamp(new SecondTimeStamp(timestamp))
            .addValue(0, value++)
            .addValue(1, 1)
            .build();
        ts.addValue(summary);
        timestamp += 3600;
      }

      results.add(new Pair<>(result, ts));
    } else {
      timestamp += 86400;
    }
    return results;
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.ExecutionBuilder;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipeline;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.context.QueryContext2;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.MockDataStore.MockRow;
import net.opentsdb.storage.MockDataStore.MockSpan;
import net.opentsdb.utils.Config;

public class TestMockStore {

  private TSDB tsdb;
  private Config config;
  private QueryPipelineContext context;
 
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    config = new Config(false);
    context = mock(QueryPipelineContext.class);
    when(tsdb.getConfig()).thenReturn(config);
    
    config.overrideConfig("MockDataStore.timestamp", "1483228800000");
  }
  
  @Test
  public void foo() throws Exception {
    config.overrideConfig("MockDataStore.threadpool.enable", "true");
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483228800000")
            .setEnd("1483236000000"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
    class TestListener implements QueryListener {
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      @Override
      public void onComplete() {
        // TODO Auto-generated method stub
        System.out.println("COMPLETE");
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        System.out.println("GOT DATA");
        for (TimeSeries ts : next.timeSeries()) {
          System.out.println(ts.id());
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          if (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            System.out.println("   " + v.timestamp().epoch() + "  " + v.value().toDouble());
          }
        }
        //ctx.fetchNext();
      }

      @Override
      public void onError(Throwable t) {
        // TODO Auto-generated method stub
        
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = new ExecutionBuilder()
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .setExecutor(mds)
        .setQueryListener(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join();
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleOneMetric() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
    class TestListener implements QueryListener {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(4, next.timeSeries().size());
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          
          assertEquals("sys.cpu.user", ts.id().metrics().get(0));
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals((end_ts - start_ts) / MockDataStore.INTERVAL, values);
        }
        on_next++;
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = new ExecutionBuilder()
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .setExecutor(mds)
        .setQueryListener(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join();
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void querySingleTwoMetrics() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
    class TestListener implements QueryListener {
      int on_next = 0;
      int on_error = 0;
      Deferred<Object> completed = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        assertEquals(8, next.timeSeries().size());
        int i = 0;
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = start_ts;
          int values = 0;
          
          if (i > 3) {
            assertEquals("web.requests", ts.id().metrics().get(0));
          } else {
            assertEquals("sys.cpu.user", ts.id().metrics().get(0));
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals((end_ts - start_ts) / MockDataStore.INTERVAL, values);
          i++;
        }
        on_next++;
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = new ExecutionBuilder()
        .setQuery(query)
        .setMode(QueryMode.SINGLE)
        .setExecutor(mds)
        .setQueryListener(listener)
        .build();
    ctx.fetchNext();
    
    listener.completed.join();
    assertEquals(1, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void queryClientStream() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    long start_ts = 1483228800000L;
    long end_ts = 1483236000000l;
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart(Long.toString(start_ts))
            .setEnd(Long.toString(end_ts)))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
    class TestListener implements QueryListener {
      int on_next = 0;
      int on_error = 0;
      int offset = 0;
      QueryContext ctx;
      Deferred<Object> completed = new Deferred<Object>();
      
      @Override
      public void onComplete() {
        completed.callback(null);
      }

      @Override
      public void onNext(QueryResult next) {
        if (next.parallelId() == 0 && on_next > 0) {
          offset++;
        }
        assertEquals(4, next.timeSeries().size());
        int i = 0;
        for (TimeSeries ts : next.timeSeries()) {
          long timestamp = end_ts - ((offset + 1) * MockDataStore.ROW_WIDTH);
          int values = 0;
          
          if (on_next % 2 == 0) {
            assertEquals("sys.cpu.user", ts.id().metrics().get(0));
          } else {
            assertEquals("web.requests", ts.id().metrics().get(0));
          }
          Iterator<TimeSeriesValue<?>> it = ts.iterator(NumericType.TYPE).get();
          while (it.hasNext()) {
            TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
            assertEquals(timestamp, v.timestamp().msEpoch());
            timestamp += MockDataStore.INTERVAL;
            values++;
          }
          assertEquals(MockDataStore.ROW_WIDTH / MockDataStore.INTERVAL, values);
          i++;
        }
        on_next++;
        ctx.fetchNext();
      }

      @Override
      public void onError(Throwable t) {
        on_error++;
      }
      
    }
    
    TestListener listener = new TestListener();
    QueryContext ctx = new ExecutionBuilder()
        .setQuery(query)
        .setMode(QueryMode.CLIENT_STREAM)
        .setExecutor(mds)
        .setQueryListener(listener)
        .build();
    listener.ctx = ctx;
    ctx.fetchNext();
    
    listener.completed.join();
    assertEquals(4, listener.on_next);
    assertEquals(0, listener.on_error);
    mds.shutdown().join();
  }
  
  @Test
  public void initialize() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    for (final Entry<TimeSeriesId, MockSpan> series : mds.getDatabase().entrySet()) {
      assertEquals(24, series.getValue().rows().size());
      
      long ts = 1483228800000L;
      for (MockRow row : series.getValue().rows()) {
        assertEquals(ts, row.base_timestamp);
        
        Iterator<TimeSeriesValue<?>> it = row.iterator(NumericType.TYPE).get();
        int count = 0;
        while (it.hasNext()) {
          assertEquals(ts + (count * 60000), it.next().timestamp().msEpoch());
          count++;
        }
        ts += MockDataStore.ROW_WIDTH;
        assertEquals(60, count);
      }
      assertEquals(1483315200000L, ts);
    }
  }
  
  @Test
  public void write() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .addMetric("unit.test")
        .addTags("dc", "lga")
        .addTags("host", "db01")
        .build();
    MutableNumericType dp = new MutableNumericType();
    TimeStamp ts = new MillisecondTimeStamp(1483228800000L);
    dp.reset(ts, 42.5);
    mds.write(id, dp, null, null);
    assertEquals((4 * 4 * 4) + 1, mds.getDatabase().size());
    
    ts.updateMsEpoch(1483228800000L + 60000L);
    dp.reset(ts, 24.5);
    mds.write(id, dp, null, null);
    
    // no out-of-order timestamps per series for now. at least within a "row".
    ts.updateMsEpoch(1483228800000L + 30000L);
    dp.reset(ts, -1);
    try {
      mds.write(id, dp, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}

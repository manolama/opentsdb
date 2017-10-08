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
package net.opentsdb.query.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.iterators.IteratorTestUtils;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.context.QueryContext2;
import net.opentsdb.query.execution.StorageQueryExecutor.Config;
import net.opentsdb.query.execution.TestQueryExecutor.MockPipeline;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.JSON;

public class TestStorageQueryExecutor extends BaseExecutorTest {
  
  private MockPipeline execution;
  private TimeSeriesDataStore store;
  private QueryPipelineContext context;
  private Config config;
  private long ts_start;
  private long ts_end;
//  
//  @Before
//  public void beforeLocal() {
//    node = mock(ExecutionGraphNode.class);
//    store = mock(TimeSeriesDataStore.class);
//    context = mock(QueryPipelineContext.class);
//    config = (Config) Config.newBuilder()
//        .setStorageId("MockStore")
//        .setExecutorId("LocalStore")
//        .setExecutorType("StorageQueryExecutor")
//        .build();
//    
//    when(node.getDefaultConfig()).thenReturn(config);
//    when(node.graph()).thenReturn(graph);
//    when(registry.getPlugin(eq(TimeSeriesDataStore.class), anyString()))
//      .thenReturn(store);
//    
//    ts_start = 1483228800000L;
//    ts_end = 1483236000000L;
//    query = TimeSeriesQuery.newBuilder()
//        .setTime(Timespan.newBuilder()
//            .setStart(Long.toString(ts_start))
//            .setEnd(Long.toString(ts_end)))
//        .addMetric(Metric.newBuilder()
//            .setMetric("sys.cpu.user")
//            .setFilter("f1")
//            .setId("m1"))
//        .addFilter(Filter.newBuilder()
//            .setId("f1")
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("web01")
//                .setType("literal_or")
//                .setTagk("host")
//                )
//            .addFilter(TagVFilter.newBuilder()
//                .setFilter("PHX")
//                .setType("literal_or")
//                .setTagk("dc")
//                )
//            )
//        .build();
//    execution = new MockPipeline();
//    when(store.executeQuery(context)).thenReturn(execution);
//  }
//  
//  @Test
//  public void ctor() throws Exception {
//    try {
//      new StorageQueryExecutor(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    // no default config
//    node = mock(ExecutionGraphNode.class);
//    try {
//      new StorageQueryExecutor(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    // no such store
//    node = mock(ExecutionGraphNode.class);
//    when(node.getDefaultConfig()).thenReturn(config);
//    when(registry.getPlugin(eq(TimeSeriesDataStore.class), anyString()))
//      .thenReturn(null);
//    try {
//      new StorageQueryExecutor(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//  
//  @Test
//  public void executeGoodQuery() throws Exception {
//    final StorageQueryExecutor executor = new StorageQueryExecutor(node);
//    
//    AtomicBoolean completed = new AtomicBoolean();
//    AtomicBoolean fetched = new AtomicBoolean();
//    class TestListener implements QueryListener {
//
//      @Override
//      public void onComplete() {
//        completed.set(true);
//      }
//
//      @Override
//      public void onNext(QueryResult next) {
//        IteratorTestUtils.validateData(next, ts_start, ts_end, 0, 1000);
//        fetched.set(true);
//      }
//
//      @Override
//      public void onError(Throwable t) { }
//      
//    }
//    
//    TestListener listener = new TestListener();
//    when(context.getListener()).thenReturn(listener);
//    
//    final QueryNode exec = executor.executeQuery(context);
//    assertEquals(1, executor.outstandingPipelines().size());
//    assertEquals(0, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertFalse(completed.get());
//    assertFalse(fetched.get());
//    
//    exec.fetchNext(0);
//    assertEquals(1, executor.outstandingPipelines().size());
//    assertEquals(1, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertFalse(completed.get());
//    assertFalse(fetched.get());
//    
//    execution.getListener().onNext(IteratorTestUtils.generateData(ts_start, ts_end, 1000));
//    assertEquals(1, executor.outstandingPipelines().size());
//    assertEquals(1, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertFalse(completed.get());
//    assertTrue(fetched.get());
//    
//    exec.fetchNext(0);
//    execution.getListener().onComplete();
//    assertEquals(0, executor.outstandingPipelines().size());
//    assertEquals(2, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertTrue(completed.get());
//    assertTrue(fetched.get());
//  }
//  
//  @Test
//  public void executeException() throws Exception {
//    final StorageQueryExecutor executor = new StorageQueryExecutor(node);
//    AtomicBoolean completed = new AtomicBoolean();
//    AtomicBoolean fetched = new AtomicBoolean();
//    class TestListener implements QueryListener {
//      Throwable t;
//      
//      @Override
//      public void onComplete() {
//        completed.set(true);
//      }
//
//      @Override
//      public void onNext(QueryResult next) {
//        fetched.set(true);
//      }
//
//      @Override
//      public void onError(Throwable t) { 
//        this.t = t;
//      }
//      
//    }
//    
//    TestListener listener = new TestListener();
//    when(context.getListener()).thenReturn(listener);
//    
//    executor.executeQuery(context);
//    assertEquals(1, executor.outstandingPipelines().size());
//    assertEquals(0, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertFalse(completed.get());
//    assertFalse(fetched.get());
//    
//    execution.getListener().onError(new IllegalStateException("Boo!"));
//    assertEquals(0, executor.outstandingPipelines().size());
//    assertEquals(0, execution.fetched_next);
//    assertEquals(0, execution.closed);
//    assertFalse(completed.get());
//    assertFalse(fetched.get());
//    assertTrue(listener.t instanceof IllegalStateException);
//  }
//  
  @Test
  public void executeClose() throws Exception {
    final StorageQueryExecutor executor = new StorageQueryExecutor(node);
    AtomicBoolean completed = new AtomicBoolean();
    AtomicBoolean fetched = new AtomicBoolean();
    class TestListener implements QueryListener {
      
      @Override
      public void onComplete() {
        completed.set(true);
      }

      @Override
      public void onNext(QueryResult next) {
        fetched.set(true);
      }

      @Override
      public void onError(Throwable t) { }
      
    }
    
    TestListener listener = new TestListener();
    when(context.getListener()).thenReturn(listener);
    final QueryNode exec = executor.executeQuery(context);
    assertEquals(1, executor.outstandingPipelines().size());
    assertEquals(0, execution.fetched_next);
    assertEquals(0, execution.closed);
    assertFalse(completed.get());
    assertFalse(fetched.get());
    
    exec.close();
    assertEquals(0, executor.outstandingPipelines().size());
    assertEquals(0, execution.fetched_next);
    assertEquals(1, execution.closed);
    assertTrue(completed.get());
    assertFalse(fetched.get());
  }
  
  @Test
  public void builder() throws Exception {
    String json = JSON.serializeToString(config);
    assertTrue(json.contains("\"executorType\":\"StorageQueryExecutor\""));
    assertTrue(json.contains("\"storageId\":\"MockStore\""));
    assertTrue(json.contains("\"executorId\":\"LocalStore\""));
    
    json = "{\"executorType\":\"StorageQueryExecutor\",\"storageId\":"
        + "\"MockStore\",\"executorId\":\"LocalStore\"}";
    config = JSON.parseToObject(json, Config.class);
    assertEquals("StorageQueryExecutor", config.executorType());
    assertEquals("MockStore", config.getStorageId());
    assertEquals("LocalStore", config.getExecutorId());
  }
  
  @Test
  public void hashCodeEqualsCompareTo() throws Exception {
    final Config c1 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    
    Config c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertEquals(c1.hashCode(), c2.hashCode());
    assertEquals(c1, c2);
    assertEquals(0, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("RemoteStore") // <-- Diff
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("DiffStore") // <-- Diff
        .setExecutorType("StorageQueryExecutor")
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(1, c1.compareTo(c2));
    
    c2 = (Config) Config.newBuilder()
        .setStorageId("MockStore")
        .setExecutorId("LocalStore")
        .setExecutorType("StorageQueryExecutor2") // <-- Diff
        .build();
    assertNotEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c2);
    assertEquals(-1, c1.compareTo(c2));
  }
}

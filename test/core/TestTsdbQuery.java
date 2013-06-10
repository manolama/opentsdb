// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class, 
  Scanner.class, TsdbQuery.class})
public final class TestTsdbQuery {
  private Config config;
  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private TsdbQuery query = null;
  private MockBase storage;

  @Before
  public void before() throws Exception {
    config = new Config(false);
    tsdb = new TSDB(config);
    query = new TsdbQuery(tsdb);
    
    // replace the "real" field objects with mocks
    Field cl = tsdb.getClass().getDeclaredField("client");
    cl.setAccessible(true);
    cl.set(tsdb, client);
    
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);
    
    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);
    
    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);
    
    // mock UniqueId
    when(metrics.getId("sys.cpu.user"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(tag_names.getId("host"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getOrCreateId("host"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getId("dc"))
      .thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getOrCreateId("web01"))
      .thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getId("web02"))
      .thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getOrCreateId("web02"))
      .thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));
    
    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }
  
  @Test
  public void setStartTime() throws Exception {
    query.setStartTime(1356998400L);
    assertEquals(1356998400L, query.getStartTime());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidNegative() throws Exception {
    query.setStartTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeInvalidTooBig() throws Exception {
    query.setStartTime(4294967296L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeEqualtoEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setStartTimeGreaterThanEndTime() throws Exception {
    query.setEndTime(1356998400L);
    query.setStartTime(1356998460L);
  }
  
  @Test
  public void setEndTime() throws Exception {
    query.setEndTime(1356998400L);
    assertEquals(1356998400L, query.getEndTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void getStartTimeNotSet() throws Exception {
    query.getStartTime();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidNegative() throws Exception {
    query.setEndTime(-1L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeInvalidTooBig() throws Exception {
    query.setEndTime(4294967296L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeEqualtoEndTime() throws Exception {
    query.setStartTime(1356998400L);
    query.setEndTime(1356998400L);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setEndTimeGreaterThanEndTime() throws Exception {
    query.setStartTime(1356998460L);
    query.setEndTime(1356998400L);
  }
  
  @Test
  public void getEndTimeNotSet() throws Exception {
    PowerMockito.mockStatic(System.class);
    when(System.currentTimeMillis()).thenReturn(1357300800000L);
    assertEquals(1357300800L, query.getEndTime());
  }
  
  @Test
  public void setTimeSeries() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NullPointerException.class)
  public void setTimeSeriesNullTags() throws Exception {
    query.setTimeSeries("sys.cpu.user", null, Aggregators.SUM, false);
  }
  
  @Test
  public void setTimeSeriesEmptyTags() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchMetric() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagk() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("dc", "web01");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagv() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web03");
    query.setTimeSeries("sys.cpu.system", tags, Aggregators.SUM, false);
  }

  @Test
  public void setTimeSeriesTS() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000001000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
    assertNotNull(query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSNullList() throws Exception {
    query.setTimeSeries(null, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSEmptyList() throws Exception {
    final List<String> tsuids = new ArrayList<String>();
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setTimeSeriesTSDifferentMetrics() throws Exception {
    final List<String> tsuids = new ArrayList<String>(2);
    tsuids.add("000001000001000001");
    tsuids.add("000002000001000002");
    query.setTimeSeries(tsuids, Aggregators.SUM, false);
  }
  
  @Test
  public void downsample() throws Exception {
    query.downsample(60, Aggregators.SUM);
    assertNotNull(query);
  }
  
  @Test (expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    query.downsample(60, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    query.downsample(0, Aggregators.SUM);
  }
  
  @Test
  public void runLongSingleTS() throws Exception {
    storeTwoLongTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
  }
  
  @Test
  public void runLongTwoAggSum() throws Exception {
    storeTwoLongTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    
    for (DataPoint dp : dps[0]) {
      assertEquals(301, dp.longValue());
    }
  }
  
  @Test
  public void runLongTwoGroup() throws Exception {
    storeTwoLongTimeSeries();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "*");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(2, dps.length);
    
    int value = 1;
    for (DataPoint dp : dps[0]) {
      assertEquals(value, dp.longValue());
      value++;
    }
    
    value = 300;
    for (DataPoint dp : dps[1]) {
      assertEquals(value, dp.longValue());
      value--;
    }
  }
  
  @Test (expected = IllegalStateException.class)
  public void runSingleTSStartNotSet() throws Exception {
    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    query.run();
  }

  private void setQueryStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));
  }
  
  private void storeTwoLongTimeSeries() throws Exception {
    setQueryStorage();
    // dump a bunch of rows
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
    }
  }
}

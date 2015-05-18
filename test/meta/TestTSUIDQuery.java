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
package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class, 
  GetRequest.class, PutRequest.class, DeleteRequest.class, KeyValue.class, 
  Scanner.class, TSMeta.class, AtomicIncrementRequest.class, System.class, 
  TSUIDQuery.class})
public class TestTSUIDQuery extends BaseUIDMetaTest {

  @Test
  public void setQuery() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test
  public void setQueryEmtpyTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.system", new HashMap<String, String>(0));
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagk() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("dc", "web01");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void setQueryNSUTagv() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web03");
    query.setQuery("sys.cpu.user", tags);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getLastWriteTimesQueryNotSet() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getLastWriteTimes().joinUninterruptibly();
  }

  @Test
  public void getTSMetasSingle() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    tags.put("host", "web01");
    query.setQuery("sys.cpu.user", tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMulti() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery("sys.cpu.user", tags);
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(2, tsmetas.size());
  }
  
  @Test
  public void getTSMetasMultipleTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    HashMap<String, String> tags = new HashMap<String, String>();
    query.setQuery("sys.cpu.nice", tags);
    tags.put("host", "web01");
    tags.put("datacenter", "dc01");
    List<TSMeta> tsmetas = query.getTSMetas().joinUninterruptibly();
    assertEquals(1, tsmetas.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getTSMetasNullMetric() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.getTSMetas().joinUninterruptibly();
  }

  // TODO - Need to split mockbase by table. For now it's all in the same table
  // so these tests are cheating a bit since we're storing different column
  // families in the same table. It'll do for now.
  @Test
  public void getLastPointCounterMeta() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 0, 0).join();
    assertEquals(1388534400015L, dp.getTimestamp());
    assertEquals("42", dp.getValue());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
  }
  
  @Test
  public void getLastPointCounterMetaTwoPoints() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    tsdb.addPoint("sys.cpu.user", 1388534400016L, 24, tags);
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 0, 0).join();
    assertEquals(1388534400016L, dp.getTimestamp());
    assertEquals("24", dp.getValue());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
  }
  
  @Test
  public void getLastPointCounterMetaCompacted() throws Exception {
    final byte[] key = new byte[] { 0, 0, 1, 0x52, (byte) 0xC3, 0x5A, 
        (byte) 0x80, 0, 0, 1, 0, 0, 1 };
    storage.addColumn(key, "t".getBytes(), new byte[] { 0, 0,  0, 0x10 }, 
        new byte[] { 0x2A, 0x18, 0 });
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 0, 0).join();
    assertEquals(1388534401000L, dp.getTimestamp());
    assertEquals("24", dp.getValue());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
  }
  
  @Test
  public void getLastPointCounterMetaDupeCompacted() throws Exception {
    final byte[] key = new byte[] { 0, 0, 1, 0x52, (byte) 0xC3, 0x5A, 
        (byte) 0x80, 0, 0, 1, 0, 0, 1 };
    storage.addColumn(key, "t".getBytes(), new byte[] { 0, 0,  0, 0x10 }, 
        new byte[] { 0x2A, 0x18, 0 });
    tsdb.addPoint("sys.cpu.user", 1388534401L, 42.5, tags);
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 0, 0).join();
    assertEquals(1388534401000L, dp.getTimestamp());
    assertEquals("42.5", dp.getValue());
    assertNull(dp.getMetric());
    assertNull(dp.getTags());
  }
  
  @Test
  public void getLastPointCounterMetaResolve() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        true, 0, 0).join();
    assertEquals(1388534400015L, dp.getTimestamp());
    assertEquals("42", dp.getValue());
    assertEquals("sys.cpu.user", dp.getMetric());
    assertEquals("web01", dp.getTags().get("host"));
  }
  
  @Test
  public void getLastPointCounterMetaNoValue() throws Exception {
    assertNull(TSUIDQuery.getLastPoint(tsdb, TSUID, false, 0, 0).join());
  }
  
  @Test (expected = RuntimeException.class)
  public void getLastPointNoSuchMetaTable() throws Exception {
    Mockito.doThrow(new RuntimeException("Table does not exist"))
      .when(client).get(any(GetRequest.class));
    TSUIDQuery.getLastPoint(tsdb, TSUID, false, 0, 0).join();
  }

  @Test
  public void getLastPointTimeGiven() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 0, 1388534400).join();
    assertEquals(1388534400015L, dp.getTimestamp());
    assertEquals("42", dp.getValue());
  }
  
  @Test
  public void getLastPointCounterMetaNoCounter() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    storage.flushColumn(TSUID, NAME_FAMILY, "ts_ctr".getBytes(MockBase.ASCII()));
    assertNull(TSUIDQuery.getLastPoint(tsdb, TSUID, false, 0, 0).join());
  }
  
  @Test
  public void getLastPointMaxLookups() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388534400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    
    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 2, 0).join();
    assertEquals(1388534400015L, dp.getTimestamp());
    assertEquals("42", dp.getValue());
  }
  
  @Test
  public void getLastPointMaxLookupsScanBack() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388538000000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);

    final IncomingDataPoint dp = TSUIDQuery.getLastPoint(tsdb, TSUID, 
        false, 2, 0).join();
    assertEquals(1388534400015L, dp.getTimestamp());
    assertEquals("42", dp.getValue());
  }
  
  @Test
  public void getLastPointMaxLookupsScanBackNotFarEnough() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388545200000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);

    assertNull(TSUIDQuery.getLastPoint(tsdb, TSUID, false, 2, 0).join());
  }
  
  @Test
  public void getLastPointMaxLookupsScanBackNotFarEnoughAndTime() throws Exception {
    // scanbacks take precedence
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388545200000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);

    assertNull(TSUIDQuery.getLastPoint(tsdb, TSUID, false, 2, 1388534400L).join());
  }
  
  @Test
  public void getLastPointMaxLookupsNothingFound() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388534400015L);

    assertNull(TSUIDQuery.getLastPoint(tsdb, TSUID, false, 2, 0).join());
  }

  @Test (expected = NullPointerException.class)
  public void getLastPointNullTSDB() throws Exception {
    TSUIDQuery.getLastPoint(null, TSUID, false, 0, 0).join();
  }
  
  @Test
  public void getLastWriteTimes() throws Exception {
    query = new TSUIDQuery(tsdb);
    query.setQuery("sys.cpu.user", Collections.<String, String>emptyMap());
    final ByteMap<Long> times = query.getLastWriteTimes().join();
    assertEquals(2, times.size());
    assertEquals((Long)1388534400015L, times.get(TSUID));
    assertEquals((Long)1388534400017L, times.get(TSUID2));
  }
  
  @Test
  public void getLastWriteTimesWithTags() throws Exception {
    query = new TSUIDQuery(tsdb);
    final HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setQuery("sys.cpu.user", tags);
    final ByteMap<Long> times = query.getLastWriteTimes().join();
    assertEquals(1, times.size());
    assertEquals((Long)1388534400015L, times.get(TSUID));
  }
}

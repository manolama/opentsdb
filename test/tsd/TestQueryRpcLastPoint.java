// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.nio.charset.Charset;

import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.meta.BaseUIDMetaTest;
import net.opentsdb.utils.Config;

import org.hbase.async.GetRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, HttpQuery.class, Query.class, 
  Deferred.class, TSQuery.class, GetRequest.class, System.class })
public class TestQueryRpcLastPoint extends BaseUIDMetaTest {

  private static String TSUID_STRING = "000001000001000001";
  private static String TSUID2_STRING = "000001000001000002";
  
  @Test
  public void tsuidQueryCounterMeta() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertFalse(json.contains("\"tsuid\":\"" + TSUID2_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
  
  @Test
  public void tsuidQueryCounterMetaResolve() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?resolve=true&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    System.out.println(json);
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
  }
  
  @Test
  public void tsuidQueryCounterMetaNoValue() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?resolve=true&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertEquals("[]", json);
  }

  @Test (expected = RuntimeException.class)
  public void tsuidQueryNoSuchMetaTable() throws Exception {
    Mockito.doThrow(new RuntimeException("Table does not exist"))
      .when(client).get(any(GetRequest.class));
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?resolve=true&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
  }
  
  @Test
  public void tsuidQueryBackScan() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388534400000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?back_scan=2&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
  
  @Test
  public void tsuidQueryBackScanIterate() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388538000000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?back_scan=2&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
  
  @Test
  public void tsuidQueryBackScanIterateNotFarEnough() throws Exception {
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388545200000L);
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?back_scan=2&tsuids=" + TSUID_STRING);
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertEquals("[]", json);
  }

  @Test
  public void metricQuery() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 24, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));

    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID2_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
  
  @Test
  public void metricQueryResolve() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 24, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?resolve=true&timeseries=sys.cpu.user");
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    System.out.println(json);
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID2_STRING + "\""));
    assertTrue(json.contains("\"metric\":\"sys.cpu.user\""));
    assertTrue(json.contains("\"tags\":{\"host\":\"web01\"}"));
    assertTrue(json.contains("\"tags\":{\"host\":\"web02\"}"));
  }
  
  @Test
  public void metricQueryWithTag() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 24, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user{host=web01}");
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));

    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertFalse(json.contains("\"tsuid\":\"" + TSUID2_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
  
  @Test
  public void metricQueryNoData() throws Exception {
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?timeseries=sys.cpu.user");
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertEquals("[]", json);
  }

  @Test
  public void mixedQuery() throws Exception {
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 42, tags);
    tags.put("host", "web02");
    tsdb.addPoint("sys.cpu.user", 1388534400015L, 24, tags);
    final HttpQuery query = NettyMocks.getQuery(tsdb, 
        "/api/query/last?tsuids=" + TSUID_STRING + 
        "&timeseries=sys.cpu.user{host=web02}");
    new QueryRpc().execute(tsdb, query);
    final String json = query.response().getContent()
        .toString(Charset.forName("UTF-8"));
    assertTrue(json.contains("\"timestamp\":1388534400015"));
    assertTrue(json.contains("\"value\":\"42\""));
    assertTrue(json.contains("\"value\":\"24\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID_STRING + "\""));
    assertTrue(json.contains("\"tsuid\":\"" + TSUID2_STRING + "\""));
    assertFalse(json.contains("metric"));
    assertFalse(json.contains("tags"));
  }
}

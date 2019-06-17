// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.Downsample.DownsampleResult;

public class TestDownsamplePush {
  
  private static MockTSDB TSDB;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  private static NumericSummaryInterpolatorConfig SUMMARY_CONFIG;
  private static Map<Integer, DownsamplePartialTimeSeriesSet> SETS;
  
  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private DownsampleConfig config;
  private QueryNode upstream;
  private SemanticQuery query;
  private List<TimeSeriesDataSource> sources;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = spy(new DefaultRegistry(TSDB));
    ((DefaultRegistry) TSDB.registry).initialize(true);
    
    if (!TSDB.getConfig().hasProperty("tsd.storage.enable_push")) {
      TSDB.getConfig().register("tsd.storage.enable_push", true, false, "TEST");
    }
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
      .setFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
      .setDataType(NumericType.TYPE.toString())
      .build();
  
    SUMMARY_CONFIG = 
          (NumericSummaryInterpolatorConfig) NumericSummaryInterpolatorConfig.newBuilder()
      .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
      .setDefaultRealFillPolicy(FillWithRealPolicy.NEXT_ONLY)
      .addExpectedSummary(0)
      .setDataType(NumericSummaryType.TYPE.toString())
      .build();
    
    SETS = Maps.newHashMap();
    
    ObjectPool mock_pool = mock(ObjectPool.class);
    doReturn(mock_pool).when(TSDB.registry).getObjectPool(
        DownsamplePartialTimeSeriesSetPool.TYPE);
    when(mock_pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        final PooledObject obj = mock(PooledObject.class);
        final DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
        doAnswer(new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            SETS.put((int) invocation.getArguments()[2], set);
            return null;
          }
        }).when(set).reset(any(Downsample.class), anyString(), anyInt());
        when(obj.object()).thenReturn(set);
        return obj;
      }
    });
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = mock(DownsampleFactory.class);
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(context.tsdb()).thenReturn(TSDB);
    sources = Lists.newArrayList();
    SETS.clear();
  }
  
  @Test
  public void initOneSource1hQueryTimeLessThanSegment() throws Exception {
    // 12:16 to 12:30
    setConfig(1559996160, 1559997000, "15s");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(15000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(15000, ds.interval_ms);
    
    // runall
    setConfig(1559996160, 1559997000, "0all");
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeEqualsSegment() throws Exception {
    // 12:00 to 13:00
    setConfig(1559995200, 1559998800, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559995200, 1559998800, "0all");
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeOverlapsSegments() throws Exception {
    // 12:30 to 13:30
    setConfig(1559997000, 1560000600, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(5, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(2, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(2, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559997000, 1560000600, "0all");
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(5, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(2, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(2, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeStartsAtSegmentEndsWithinSegment() throws Exception {
    // 12:00 to 12:30
    setConfig(1559995200, 1559997000, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559995200, 1559997000, "0all");
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeStartInSegmentEndsAtSegment() throws Exception {
    // 12:30 to 13:00
    setConfig(1559997000, 1559998800, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559997000, 1559998800, "0all");
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeSpans1Segment() throws Exception {
    // 12:30 to 14:30
    setConfig(1559997000, 1560004200, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(6, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(3, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    assertEquals(1560002400, sizes[5]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(3, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559997000, 1560004200, "0all");
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(6, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(3, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    assertEquals(1560002400, sizes[5]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(3, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hQueryTimeSpans2Segments() throws Exception {
    // 12:30 to 15:30
    setConfig(1559997000, 1560007800, "1m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(7, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(4, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    assertEquals(1560002400, sizes[5]);
    assertEquals(1560006000, sizes[6]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(4, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    assertNull(ref.get(3));
    
    assertEquals(60000, ds.interval_ms);
    
    // runall
    setConfig(1559997000, 1560007800, "0all");
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(7, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(4, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    assertEquals(1559998800, sizes[4]);
    assertEquals(1560002400, sizes[5]);
    assertEquals(1560006000, sizes[6]);
    
    ref = ds.sets.get("src1");
    assertEquals(4, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    assertNull(ref.get(3));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hWouldReturnNoData() throws Exception {
    // 12:29 to 12:30
    setConfig(1559996940, 1559998800, "30m");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(1800000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(1800000, ds.interval_ms);
    
    // runall
    setConfig(1559996940, 1559998800, "0all");
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(0, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(0, ds.interval_ms);
  }
  
  @Test
  public void initOneSource2hQueryTimeLessThanSegment() throws Exception {
    // 12:16 to 12:30
    setConfig(1559996160, 1559997000, "15s");
    setupSource("src1", 1559995200, new String[] { "2h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(15000, sizes[0]);
    assertEquals(7200000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(15000, ds.interval_ms);
    
    // Diff alignment times still snap to the previous hour.
    // 13:16 to 13:30
    setConfig(1559999760, 1560000600, "15s");
    setupSource("src1", 1559995200, new String[] { "2h" });
    
    ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(15000, sizes[0]);
    assertEquals(7200000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    ref = ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(15000, ds.interval_ms);
  }
  
  @Test
  public void initSourceWithNullIntervals() throws Exception {
    setConfig(1559996160, 1559997000, "15s");
    setupSource("src1", 1559995200, null);
    
    Downsample ds = new Downsample(factory, context, config);
    try {
      ds.initialize(null).join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void initSourceWithInvalidIntervalUnits() throws Exception {
    setConfig(1559996160, 1559997000, "15s");
    setupSource("src1", 1559995200, new String[] { "60m" });
    
    Downsample ds = new Downsample(factory, context, config);
    try {
      ds.initialize(null).join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void initOneSource1dQueryRollups() throws Exception {
    // 6/7 13:15 to 6/9 1:33
    setConfig(1559913300, 1560043980, "1m");
    setupSource("src1", 1559865600, new String[] { "1d", "6h", "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(6, sizes.length);
    assertEquals(60000, sizes[0]);
    assertEquals(86400000, sizes[1]);
    assertEquals(3, sizes[2]);
    assertEquals(1559865600, sizes[3]);
    assertEquals(1559952000, sizes[4]);
    assertEquals(1560038400, sizes[5]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(3, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    
    assertEquals(60000, ds.interval_ms);
  }
  
  @Test
  public void initTwoSource() throws Exception {
    // 12:16 to 12:30
    setConfig(1559996160, 1559997000, "15s");
    
    TimeSeriesDataSource src1 = mock(TimeSeriesDataSource.class);
    sources.add(src1);
    QueryNodeConfig cfg = mock(QueryNodeConfig.class);
    when(cfg.getId()).thenReturn("src1");
    when(src1.config()).thenReturn(cfg);
    when(src1.setIntervals()).thenReturn(new String[] { "1h" });
    when(src1.firstSetStart()).thenReturn(new SecondTimeStamp(1559995200));
    
    TimeSeriesDataSource src2 = mock(TimeSeriesDataSource.class);
    sources.add(src2);
    QueryNodeConfig cfg2 = mock(QueryNodeConfig.class);
    when(cfg2.getId()).thenReturn("src2");
    when(src2.config()).thenReturn(cfg2);
    when(src2.setIntervals()).thenReturn(new String[] { "2h" });
    when(src2.firstSetStart()).thenReturn(new SecondTimeStamp(1559995200));
    when(context.downstreamSources(any(QueryNode.class))).thenReturn(sources);
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(2, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(15000, sizes[0]);
    assertEquals(3600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    sizes = ds.set_sizes.get("src2");
    assertEquals(4, sizes.length);
    assertEquals(15000, sizes[0]);
    assertEquals(7200000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559995200, sizes[3]);
    
    assertEquals(2, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    ref = ds.sets.get("src2");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(15000, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hOddInterval45mSegmented() throws Exception {
    // 1:15 to 15:15
    setConfig(1559956500, 1560006900, "45m");
    setupSource("src1", 1559955600, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(7, sizes.length);
    assertEquals(2700000, sizes[0]);
    assertEquals(10800000, sizes[1]);
    assertEquals(4, sizes[2]);
    assertEquals(1559955600, sizes[3]);
    assertEquals(1559959200, sizes[4]);
    assertEquals(1559962800, sizes[5]);
    assertEquals(1559966400, sizes[6]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(4, ref.length());
    assertNull(ref.get(0));
    assertNull(ref.get(1));
    assertNull(ref.get(2));
    assertNull(ref.get(3));
    
    assertEquals(2700000, ds.interval_ms);
  }
  
  @Test
  public void initOneSource1hOddInterval33mOneSegment() throws Exception {
    // 1:15 to 15:15
    setConfig(1559956500, 1560006900, "33m");
    setupSource("src1", 1559955600, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    assertSame(config, ds.config());
    verify(context, times(1)).upstream(ds);
    verify(context, times(1)).downstream(ds);
    verify(context, times(1)).downstreamSources(ds);
    
    assertEquals(1, ds.set_sizes.size());
    long[] sizes = ds.set_sizes.get("src1");
    assertEquals(4, sizes.length);
    assertEquals(1980000, sizes[0]);
    assertEquals(39600000, sizes[1]);
    assertEquals(1, sizes[2]);
    assertEquals(1559955600, sizes[3]);
    
    assertEquals(1, ds.sets.size());
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertEquals(1, ref.length());
    assertNull(ref.get(0));
    
    assertEquals(1980000, ds.interval_ms);
  }
  
  @Test
  public void onNext() throws Exception {
    // 12:16 to 12:30
    setConfig(1559996160, 1559997000, "15s");
    setupSource("src1", 1559995200, new String[] { "1h" });
    
    Downsample ds = new Downsample(factory, context, config);
    ds.initialize(null).join();
    
    PartialTimeSeries pts = mockSeries(1559995200, 1559998800, "src1");
    ds.onNext(pts);
    
    assertEquals(1, SETS.size());
    DownsamplePartialTimeSeriesSet set = SETS.get(0);
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> ref = 
        ds.sets.get("src1");
    assertSame(set, ref.get(0));
    verify(set, times(1)).process(pts);
    
    PartialTimeSeries pts2 = mockSeries(1559995200, 1559998800, "src1");
    ds.onNext(pts2);
    
    assertSame(set, ref.get(0)); // found the same old one.
    verify(set, times(1)).process(pts);
    verify(set, times(1)).process(pts2);
    
    // unknown source
    PartialTimeSeries pts3 = mockSeries(1559995200, 1559998800, "src2");
    ds.onNext(pts3);
    
    assertSame(set, ref.get(0)); // found the same old one.
    verify(set, times(1)).process(pts);
    verify(set, times(1)).process(pts2);
    verify(set, never()).process(pts3);
    
    // out of bounds
    PartialTimeSeries pts4 = mockSeries(1559998800, 1560002400, "src1");
    ds.onNext(pts4);
    
    assertSame(set, ref.get(0)); // found the same old one.
    verify(set, times(1)).process(pts);
    verify(set, times(1)).process(pts2);
    verify(set, never()).process(pts3);
    verify(set, never()).process(pts4);
  }
  
  void setupSource(final String id, 
                   final long start_time, 
                   final String[] intervals) {
    TimeSeriesDataSource src1 = mock(TimeSeriesDataSource.class);
    sources.add(src1);
    QueryNodeConfig cfg = mock(QueryNodeConfig.class);
    when(cfg.getId()).thenReturn(id);
    when(src1.config()).thenReturn(cfg);
    when(src1.setIntervals()).thenReturn(intervals);
    when(src1.firstSetStart()).thenReturn(new SecondTimeStamp(start_time));
    when(context.downstreamSources(any(QueryNode.class))).thenReturn(sources);
  }
  
  PartialTimeSeries mockSeries(final long start, final long end, final String src) {
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    when(pts.set()).thenReturn(set);
    
    when(set.start()).thenReturn(new SecondTimeStamp(start));
    when(set.end()).thenReturn(new SecondTimeStamp(end));
    when(set.dataSource()).thenReturn(src);
    return pts;
  }
  
  void setConfig(final long start, 
                 final long end, 
                 final String interval) {
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Long.toString(start))
        .setEnd(Long.toString(end))
        .setExecutionGraph(Collections.emptyList())
        .build();
    when(context.query()).thenReturn(query);
    
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval(interval)
        .setStart(Long.toString(start))
        .setEnd(Long.toString(end))
        .setRunAll(interval.equals("0all") ? true : false)
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .addInterpolatorConfig(SUMMARY_CONFIG)
        .setId("foo")
        .build();
  }
}

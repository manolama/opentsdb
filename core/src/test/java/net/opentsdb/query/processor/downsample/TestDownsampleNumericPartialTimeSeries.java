package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MockNumericLongArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.utils.DateTime;

public class TestDownsampleNumericPartialTimeSeries {
  
  private static final int BASE_TIME = 1356998400;
  //30 minute offset
  final static String AF = "Asia/Kabul";
  // 12h offset w/o DST
  final static String TV = "Pacific/Funafuti";
  // 12h offset w DST
  final static String FJ = "Pacific/Fiji";
  // Tue, 15 Dec 2015 04:02:25.123 UTC
  final static long DST_TS = 1450137600000L;
  
  private static MockTSDB TSDB;
  private static List<DownsampleNumericPartialTimeSeries> SERIES;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  
  private QueryPipelineContext context;
  private DownsampleConfig config;
  private TimeSeriesQuery query;
  private Downsample node;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB();
    TSDB.registry = spy(new DefaultRegistry(TSDB));
    ((DefaultRegistry) TSDB.registry).initialize(true);
    SERIES = Lists.newArrayList();
    
    NUMERIC_CONFIG = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    query = mock(TimeSeriesQuery.class);
    node = mock(Downsample.class);
    when(node.config()).thenReturn(mock(DownsampleConfig.class));
    when(node.pipelineContext()).thenReturn(context);
    
    when(context.tsdb()).thenReturn(TSDB);
    when(context.query()).thenReturn(query);
    
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559520000));
    SERIES.clear();
  }
  
  @Test
  public void single1000seconds() throws Exception {
    final int size = 11;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 10800);
    setConfig("1000s", "avg");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME, 40)
       .addValue(BASE_TIME + 2000, 50)
       .addValue(BASE_TIME + 3600, 40)
       .addValue(BASE_TIME + 3605, 50)
       .addValue(BASE_TIME + 7200, 40)
       .addValue(BASE_TIME + 9200, 50);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    debug(dpts);
    assertArrayEquals(new double[] 
        { 40.0, Double.NaN, 50.0, 45.0, Double.NaN, Double.NaN, Double.NaN, 
            40.0, Double.NaN, 50.0, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single10seconds() throws Exception {
    final int size = 6;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("10s", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 5, 1)
       .addValue(BASE_TIME + 5 * 1, 2)
       .addValue(BASE_TIME + 5 * 3, 4)
       .addValue(BASE_TIME + 5 * 4, 8)
       .addValue(BASE_TIME + 5 * 5, 16)
       .addValue(BASE_TIME + 5 * 6, 32)
       .addValue(BASE_TIME + 5 * 7, 64)
       .addValue(BASE_TIME + 5 * 8, 128)
       .addValue(BASE_TIME + 5 * 9, 256)
       .addValue(BASE_TIME + 5 * 10, 512)
       .addValue(BASE_TIME + 5 * 11, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new long[] { 3, 4, 24, 96, 384, 1536 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void single15secondsLongs() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("15s", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 5, 1)
       .addValue(BASE_TIME + 15, 2)
       .addValue(BASE_TIME + 25, 4)
       .addValue(BASE_TIME + 35, 8)
       .addValue(BASE_TIME + 45, 16)
       .addValue(BASE_TIME + 55, 32);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new long[] { 1, 6, 8, 48 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void single15secondsDoubles() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("15s", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 5, 1.5)
       .addValue(BASE_TIME + 15, 2.75)
       .addValue(BASE_TIME + 25, 4.0)
       .addValue(BASE_TIME + 35, 8.25)
       .addValue(BASE_TIME + 45, 16.33)
       .addValue(BASE_TIME + 55, 32.6);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { 1.5, 6.75, 8.25, 48.93 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single15secondsLongAggedThenSwitchToDouble() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("15s", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 5, 1)
       .addValue(BASE_TIME + 15, 2)
       .addValue(BASE_TIME + 25, 4)
       .addValue(BASE_TIME + 35, 8.25)
       .addValue(BASE_TIME + 45, 16.33)
       .addValue(BASE_TIME + 55, 32.6);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { 1.0, 6.0, 8.25, 48.93 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single15secondsLongAggedWithToDouble() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("15s", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 5, 1)
       .addValue(BASE_TIME + 15, 2)
       .addValue(BASE_TIME + 25, 4.5)
       .addValue(BASE_TIME + 35, 8)
       .addValue(BASE_TIME + 45, 16)
       .addValue(BASE_TIME + 55, 32);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { 1.0, 6.5, 8, 48 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single2mSourceSetWiderThanDS() throws Exception {
    final int size = 2;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + (60 * 4));
    setConfig("2m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME - 60 * 2, 1)
       .addValue(BASE_TIME - 60, 2)
       .addValue(BASE_TIME, 4)
       .addValue(BASE_TIME + 60, 8)
       .addValue(BASE_TIME + 60 * 2, 16)
       .addValue(BASE_TIME + 60 * 3, 32)
       .addValue(BASE_TIME + 60 * 4, 64)
       .addValue(BASE_TIME + 60 * 5, 128)
       .addValue(BASE_TIME + 60 * 6, 256);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new long[] { 12, 48 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void single1mSourceSetWiderThanDSDataTooEarly() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("1m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME - 60 * 2, 1)
       .addValue(BASE_TIME - 60, 2);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single1mSourceSetWiderThanDSDataTooLate() throws Exception {
    final int size = 2;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + (60 * 3));
    setConfig("1m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME + 60 * 4, 64)
      .addValue(BASE_TIME + 60 * 5, 128)
      .addValue(BASE_TIME + 60 * 6, 256);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single45mSourceOddInterval() throws Exception {
    final int size = 1;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("45m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME - 60 * 2, 1)
       .addValue(BASE_TIME - 60, 2)
       .addValue(BASE_TIME, 4)
       .addValue(BASE_TIME + 60, 8)
       .addValue(BASE_TIME + 60 * 2, 16)
       .addValue(BASE_TIME + 60 * 3, 32)
       .addValue(BASE_TIME + 60 * 4, 64)
       .addValue(BASE_TIME + 60 * 5, 128)
       .addValue(BASE_TIME + 60 * 6, 256)
       .addValue(BASE_TIME + 60 * 45, 1024); // last one skipped
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new long[] { 508 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void single1mEmptySource() throws Exception {
    final int size = 2;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("1m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    
    assertArrayEquals(new double[] { Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single1mFillAtStart() throws Exception {
    final int size = 6;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + 3600 * 1);
    setConfig("10m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + (60 * 10), 4)
      .addValue(BASE_TIME + (60 * 11), 8)
      .addValue(BASE_TIME + (60 * 40), 16)
      .addValue(BASE_TIME + (60 * 45), 32)
      .addValue(BASE_TIME + (60 * 55), 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    assertArrayEquals(new double[] { Double.NaN, 12.0, Double.NaN, Double.NaN, 48.0, 64.0 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single1hMatchesSetSize() throws Exception {
    final int size = 1;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("1h", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME, 4)
       .addValue(BASE_TIME + 60, 8)
       .addValue(BASE_TIME + 60 * 2, 16)
       .addValue(BASE_TIME + 60 * 3, 32)
       .addValue(BASE_TIME + 60 * 4, 64)
       .addValue(BASE_TIME + 60 * 5, 128)
       .addValue(BASE_TIME + 60 * 6, 256)
       .addValue(BASE_TIME + 60 * 45, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    debug(dpts);
    assertArrayEquals(new long[] { 1532 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void singleRunAll() throws Exception {
    final int size = 1;
    DownsamplePartialTimeSeriesSet set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    setConfig("0all", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME, 4)
       .addValue(BASE_TIME + 60, 8)
       .addValue(BASE_TIME + 60 * 2, 16)
       .addValue(BASE_TIME + 60 * 3, 32)
       .addValue(BASE_TIME + 60 * 4, 64)
       .addValue(BASE_TIME + 60 * 5, 128)
       .addValue(BASE_TIME + 60 * 6, 256)
       .addValue(BASE_TIME + 60 * 45, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    debug(dpts);
    assertArrayEquals(new long[] { 1532 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.doubleArray());
    assertTrue(dpts.isInteger());
  }
  
  @Test
  public void multi2InOrder() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("15m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60, 8)
      .addValue(BASE_TIME + 60 * 5, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28, 96 },
        dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4) // this one is too early!
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new long[] { 28, 96, 384, 1536 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2OutOfOrder() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("15m", "sum");
    
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4) // this one is too early!
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60, 8)
      .addValue(BASE_TIME + 60 * 5, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    assertArrayEquals(new long[] { 28, 96, 384, 1536 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2InOrderFill() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + 60 * 2, 4)
      .addValue(BASE_TIME + 60 * 5, 8)
      .addValue(BASE_TIME + 60 * 9, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    double[] expected = new double[30];
    Arrays.fill(expected, Double.NaN);
    expected[2] = 4;
    expected[5] = 8;
    expected[9] = 16;
    expected[20] = 32;
    expected[25] = 64;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[2] = 4;
    expected[5] = 8;
    expected[9] = 16;
    expected[20] = 32;
    expected[25] = 64;
    expected[30] = 128;
    expected[40] = 256;
    expected[50] = 512;
    expected[55] = 1024;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2OutOfOrderFill() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60, 8)
      .addValue(BASE_TIME + 60 * 5, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[0] = 4;
    expected[1] = 8;
    expected[4] = 16;
    expected[19] = 32;
    expected[24] = 64;
    expected[29] = 128;
    expected[39] = 256;
    expected[49] = 512;
    expected[54] = 1024;
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2InOrderFillSecondNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + 60 * 2, 4)
      .addValue(BASE_TIME + 60 * 5, 8)
      .addValue(BASE_TIME + 60 * 9, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    double[] expected = new double[30];
    Arrays.fill(expected, Double.NaN);
    expected[2] = 4;
    expected[5] = 8;
    expected[9] = 16;
    expected[20] = 32;
    expected[25] = 64;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[2] = 4;
    expected[5] = 8;
    expected[9] = 16;
    expected[20] = 32;
    expected[25] = 64;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2InOrderFillFirstNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    NoDataPartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[30] = 128;
    expected[40] = 256;
    expected[50] = 512;
    expected[55] = 1024;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2InOrderBothNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    NoDataPartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2OutOfOrderFillSecondNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");
    
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + 60 * 2, 4)
      .addValue(BASE_TIME + 60 * 5, 8)
      .addValue(BASE_TIME + 60 * 9, 16)
      .addValue(BASE_TIME + 60 * 20, 32)
      .addValue(BASE_TIME + 60 * 25, 64);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[2] = 4;
    expected[5] = 8;
    expected[9] = 16;
    expected[20] = 32;
    expected[25] = 64;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2OutOfOrderFillFirstNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");

    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 30, 128)
      .addValue(BASE_TIME + 60 * 40, 256)
      .addValue(BASE_TIME + 60 * 50, 512)
      .addValue(BASE_TIME + 60 * 55, 1024);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    NoDataPartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    expected[30] = 128;
    expected[40] = 256;
    expected[50] = 512;
    expected[55] = 1024;
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi2OutOfOrderFillBothNDPTS() throws Exception {
    final int size = 60;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, BASE_TIME + 3600);
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("1m", "sum");

    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + (60 * 30), BASE_TIME + 3600);
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[0], dpts.longArray(), dpts.offset(), dpts.end());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + (60 * 30));
    NoDataPartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    double[] expected = new double[60];
    Arrays.fill(expected, Double.NaN);
    assertArrayEquals(expected, dpts.doubleArray(), dpts.offset(), dpts.end());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at45mInOrder() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("45m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 10, 8)
      .addValue(BASE_TIME + 60 * 40, 16)
      .addValue(BASE_TIME + 60 * 45, 32)
      .addValue(BASE_TIME + 60 * 55, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME + 3600 + 60 * 1, 128)
      .addValue(BASE_TIME + 3600 + 60 * 30, 256)
      .addValue(BASE_TIME + 3600 + 60 * 45, 512)
      .addValue(BASE_TIME + 3600 + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new long[] { 28, 224 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 30, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 45, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 55, 16384);
    
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28, 224, 3840, 28672 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at45mOutOfOrder() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("45m", "sum");
    
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 30, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 45, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 55, 16384);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new long[] { },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(1, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME + 3600 + 60 * 1, 128)
      .addValue(BASE_TIME + 3600 + 60 * 30, 256)
      .addValue(BASE_TIME + 3600 + 60 * 45, 512)
      .addValue(BASE_TIME + 3600 + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new long[] { },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(2, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 10, 8)
      .addValue(BASE_TIME + 60 * 40, 16)
      .addValue(BASE_TIME + 60 * 45, 32)
      .addValue(BASE_TIME + 60 * 55, 64);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28, 224, 3840, 28672 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(2, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at45mOutOfOrderDiff() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("45m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 10, 8)
      .addValue(BASE_TIME + 60 * 40, 16)
      .addValue(BASE_TIME + 60 * 45, 32)
      .addValue(BASE_TIME + 60 * 55, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 30, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 45, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 55, 16384);
    
    dpts.addSeries(pts_c);
    debug(dpts);
    assertArrayEquals(new long[] { 28 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(1, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME + 3600 + 60 * 1, 128)
      .addValue(BASE_TIME + 3600 + 60 * 30, 256)
      .addValue(BASE_TIME + 3600 + 60 * 45, 512)
      .addValue(BASE_TIME + 3600 + 60 * 55, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28, 224, 3840, 28672 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(1, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at10mInOrderFill() throws Exception {
    final int size = 18;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("10m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + (60 * 10), 4)
      .addValue(BASE_TIME + (60 * 11), 8)
      .addValue(BASE_TIME + (60 * 40), 16)
      .addValue(BASE_TIME + (60 * 45), 32)
      .addValue(BASE_TIME + (60 * 55), 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new double[] { Double.NaN, 12, Double.NaN, Double.NaN, 48, 64 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME + 3600 + 60 * 1, 128)
      .addValue(BASE_TIME + 3600 + 60 * 30, 256)
      .addValue(BASE_TIME + 3600 + 60 * 45, 512)
      .addValue(BASE_TIME + 3600 + 60 * 47, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new double[] { Double.NaN, 12, Double.NaN, Double.NaN, 48, 64,
        128, Double.NaN, Double.NaN, 256, 1536, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 20, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 35, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 39, 16384);
    
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new double[] { Double.NaN, 12, Double.NaN, Double.NaN, 48, 64,
        128, Double.NaN, Double.NaN, 256, 1536, Double.NaN,
        2048, Double.NaN, 4096, 24576, Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at10mOutOfOrderFill() throws Exception {
    final int size = 18;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("10m", "sum");
    
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 20, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 35, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 39, 16384);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new long[0],
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(1, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    MockNumericLongArrayTimeSeries pts_b = new MockNumericLongArrayTimeSeries(set_b, 42);
    pts_b.addValue(BASE_TIME + 3600 + 60 * 1, 128)
      .addValue(BASE_TIME + 3600 + 60 * 30, 256)
      .addValue(BASE_TIME + 3600 + 60 * 45, 512)
      .addValue(BASE_TIME + 3600 + 60 * 47, 1024);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new long[0],
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(2, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME + (60 * 10), 4)
      .addValue(BASE_TIME + (60 * 11), 8)
      .addValue(BASE_TIME + (60 * 40), 16)
      .addValue(BASE_TIME + (60 * 45), 32)
      .addValue(BASE_TIME + (60 * 55), 64);
    
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new double[] { Double.NaN, 12, Double.NaN, Double.NaN, 48, 64,
        128, Double.NaN, Double.NaN, 256, 1536, Double.NaN,
        2048, Double.NaN, 4096, 24576, Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(2, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at45mInOrder1NDPTS() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("45m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    MockNumericLongArrayTimeSeries pts_a = new MockNumericLongArrayTimeSeries(set_a, 42);
    pts_a.addValue(BASE_TIME, 4)
      .addValue(BASE_TIME + 60 * 10, 8)
      .addValue(BASE_TIME + 60 * 40, 16)
      .addValue(BASE_TIME + 60 * 45, 32)
      .addValue(BASE_TIME + 60 * 55, 64);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new long[] { 28, 96 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    MockNumericLongArrayTimeSeries pts_c = new MockNumericLongArrayTimeSeries(set_c, 42);
    pts_c.addValue(BASE_TIME + (3600 * 2) + 60 * 1, 2048)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 30, 4096)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 45, 8192)
      .addValue(BASE_TIME + (3600 * 2) + 60 * 55, 16384);
    
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new long[] { 28, 96, 2048, 28672 },
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  @Test
  public void multi3at45mInOrderAllNDPTS() throws Exception {
    final int size = 4;
    DownsamplePartialTimeSeriesSet ds_set = getSet(size, BASE_TIME, 
        BASE_TIME + (3600 * 3));
    when(ds_set.lastMulti()).thenReturn(1);
    setConfig("45m", "sum");
    
    PartialTimeSeriesSet set_a = getSourceSet(BASE_TIME, BASE_TIME + 3600);
    NoDataPartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(ds_set);
    dpts.addSeries(pts_a);
    debug(dpts);
    
    assertArrayEquals(new long[0],
        dpts.longArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);

    when(ds_set.lastMulti()).thenReturn(2);
    PartialTimeSeriesSet set_b = getSourceSet(BASE_TIME + 3600, BASE_TIME + (3600 * 2));
    NoDataPartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    
    dpts.addSeries(pts_b);
    debug(dpts);
    assertArrayEquals(new double[] { Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    verify(node, never()).sendUpstream(dpts);
    
    when(ds_set.allSetsAccountedFor()).thenReturn(true);
    when(ds_set.lastMulti()).thenReturn(3);
    PartialTimeSeriesSet set_c = getSourceSet(BASE_TIME + (3600 * 2), BASE_TIME + (3600 * 3));
    NoDataPartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    
    dpts.addSeries(pts_c);
    debug(dpts);
    
    assertArrayEquals(new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.series_list.size());
    assertSentAndCleanedUp(dpts);
  }
  
  DownsamplePartialTimeSeriesSet getSet(final int size, final int start, final int end) {
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(start));
    when(set.end()).thenReturn(new SecondTimeStamp(end));
    when(set.arraySize()).thenReturn(size);
    return set;
  }
  
  void setConfig(final String interval, final String agg) {
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator(agg)
        .setInterval(interval.equals("0all") ? "1m" : interval)
        .setRunAll(interval.equals("0all") ? true : false)
        .setStart(Long.toString(BASE_TIME))
        .setEnd(Long.toString(BASE_TIME + 3600))
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("foo")
        .build();
    when(node.config()).thenReturn(config);
    NumericAggregatorFactory factory = TSDB.getRegistry().getPlugin(NumericAggregatorFactory.class, agg);
    when(node.aggregator()).thenReturn(factory.newAggregator(config.getInfectiousNan()));
  }
  
  void assertArrayEquals(final long[] array, final long[] test, final int offset, final int end) {
    if  (end - offset != array.length) {
      throw new AssertionError("Array lengths differed. Expected " + (end - offset) + " but was " + array.length);
    }
    for (int i = 0; i < array.length; i++) {
      try {
        assertEquals(array[i], test[offset + i]);
      } catch (AssertionError e) {
        throw new AssertionError(e.getMessage() + " at index " + i);
      }
    }
  }
  
  void assertArrayEquals(final double[] array, final double[] test, final int offset, final int end) {
    if  (end - offset != array.length) {
      throw new AssertionError("Array lengths differed. Expected " + (end - offset) + " but was " + array.length);
    }
    for (int i = 0; i < array.length; i++) {
      try {
        assertEquals(array[i], test[offset + i], 0.001);
      } catch (AssertionError e) {
        throw new AssertionError(e.getMessage() + " at index " + i);
      }
    }
  }
  
  void assertSentAndCleanedUp(final DownsampleNumericPartialTimeSeries dpts) {
    assertNull(dpts.accumulator_array);
    assertNull(dpts.accumulator_long_array);
    assertNull(dpts.accumulator_double_array);
    verify(node, times(1)).sendUpstream(dpts);
  }
  
  PartialTimeSeriesSet getSourceSet(final int start, final int end) {
    PartialTimeSeriesSet set = mock(PartialTimeSeriesSet.class);
    when(set.start()).thenReturn(new SecondTimeStamp(start));
    when(set.end()).thenReturn(new SecondTimeStamp(end));
    return set;
  }
  
  void debug(final DownsampleNumericPartialTimeSeries dpts) {
    System.out.print("[");
    for (int i = 0; i < dpts.end(); i++) {
      if (i > 0) {
        System.out.print(", ");
      }
      if (dpts.longArray() == null) {
        System.out.print(dpts.doubleArray()[i]);
      } else {
        System.out.print(dpts.longArray()[i]);
      }
    }
    System.out.println("]  len: " + dpts.end());
  }
}

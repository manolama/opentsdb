package net.opentsdb.query.processor.downsample;


import static org.junit.Assert.assertArrayEquals;
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
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MockNumericLongArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
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
    
    ObjectPool mock_pool = mock(ObjectPool.class);
    doReturn(mock_pool).when(TSDB.registry).getObjectPool(
        DownsampleNumericPartialTimeSeriesPool.TYPE);
    when(mock_pool.claim()).thenAnswer(new Answer<PooledObject>() {
      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        PooledObject po = mock(PooledObject.class);
        DownsampleNumericPartialTimeSeries pts = mock(DownsampleNumericPartialTimeSeries.class);
        SERIES.add(pts);
        when(po.object()).thenReturn(pts);
        return po;
      }
    }); 
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    
    assertArrayEquals(new double[] 
        { 40.0, 50.0, Double.NaN, 90.0, Double.NaN, Double.NaN, Double.NaN, 40.0, Double.NaN, 50.0, Double.NaN },
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
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
    debug(dpts);
    assertArrayEquals(new double[] { 1.0, 6.5, 8, 48 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  @Test
  public void single15secondsSourceSetWiderThanDS() throws Exception {
    final int size = 2;
    DownsamplePartialTimeSeriesSet set = mock(DownsamplePartialTimeSeriesSet.class);
    when(set.node()).thenReturn(node);
    when(set.start()).thenReturn(new SecondTimeStamp(BASE_TIME));
    when(set.arraySize()).thenReturn(size);
    
    when(node.aggregator()).thenReturn(new SumFactory().newAggregator(false));
    
    setConfig("1m", "sum");
    
    MockNumericLongArrayTimeSeries pts = new MockNumericLongArrayTimeSeries(set, 42);
    pts.addValue(BASE_TIME - 60 * 2, 1)
       .addValue(BASE_TIME - 60, 2)
       .addValue(BASE_TIME, 4)
       .addValue(BASE_TIME + 60, 8)
       .addValue(BASE_TIME + 60 * 2, 16)
       .addValue(BASE_TIME + 60 * 3, 32);
    
    DownsampleNumericPartialTimeSeries dpts = new DownsampleNumericPartialTimeSeries(TSDB);
    dpts.reset(set);
    dpts.addSeries(pts);
    debug(dpts);
    assertArrayEquals(new double[] { 1.0, 6.5, 8, 48 },
        dpts.doubleArray(), dpts.offset(), dpts.end());
    assertEquals(0, dpts.offset());
    assertEquals(size, dpts.end());
    assertSentAndCleanedUp(dpts);
    assertNull(dpts.longArray());
    assertFalse(dpts.isInteger());
  }
  
  void setConfig(final String interval, final String agg) {
    config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator(agg)
        .setInterval(interval)
        .setStart(Long.toString(BASE_TIME))
        .setEnd(Long.toString(BASE_TIME + 3600))
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("foo")
        .build();
    when(node.config()).thenReturn(config);
  }
  
  void assertArrayEquals(final long[] array, final long[] test, final int offset, final int end) {
    if  (end - offset != array.length) {
      throw new AssertionError("Array lengths differed. Expected " + (end - offset) + " but was " + array.length);
    }
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], test[offset + i]);
    }
  }
  
  void assertArrayEquals(final double[] array, final double[] test, final int offset, final int end) {
    if  (end - offset != array.length) {
      throw new AssertionError("Array lengths differed. Expected " + (end - offset) + " but was " + array.length);
    }
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], test[offset + i], 0.001);
    }
  }
  
  void assertSentAndCleanedUp(final DownsampleNumericPartialTimeSeries dpts) {
    assertNull(dpts.accumulator_array);
    assertNull(dpts.accumulator_long_array);
    assertNull(dpts.accumulator_double_array);
    verify(node, times(1)).sendUpstream(dpts);
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
    System.out.println("]");
  }
}

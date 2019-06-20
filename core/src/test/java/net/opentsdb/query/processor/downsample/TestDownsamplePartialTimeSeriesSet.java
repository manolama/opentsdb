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

import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestDownsamplePartialTimeSeriesSet {

  private static MockTSDB TSDB;
  private static List<DownsampleNumericPartialTimeSeries> SERIES;
  private static NumericInterpolatorConfig NUMERIC_CONFIG;
  
  private QueryPipelineContext context;
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
  public void resetAndGetters() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setInterval("45m")
        .setStart(Long.toString(1559955600))
        .setEnd(Long.toString(1559993400))
        .addInterpolatorConfig(NUMERIC_CONFIG)
        .setId("foo")
        .build();
    when(node.config()).thenReturn(config);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        4,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // first index
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    assertEquals(4, set.totalSets());
    assertFalse(set.complete());
    assertSame(node, set.node());
    assertEquals("m1", set.dataSource());
    assertEquals(sizes[3], set.start().epoch());
    assertEquals(sizes[4], set.end().epoch());
    assertEquals(0, set.timeSeriesCount());
    assertSame(set, set.timeSpecification());
    assertEquals(config.interval(), set.interval());
    assertEquals(config.getInterval(), set.stringInterval());
    assertEquals(config.units(), set.units());
    assertEquals(Const.UTC, set.timezone());
    assertEquals(4, set.arraySize());
    
    // last index changes to the config end time.
    set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    assertEquals(4, set.totalSets());
    assertFalse(set.complete());
    assertSame(node, set.node());
    assertEquals("m1", set.dataSource());
    assertEquals(sizes[6], set.start().epoch());
    assertEquals(config.endTime().epoch(), set.end().epoch());
    assertEquals(0, set.timeSeriesCount());
    assertSame(set, set.timeSpecification());
    assertEquals(config.interval(), set.interval());
    assertEquals(config.getInterval(), set.stringInterval());
    assertEquals(config.units(), set.units());
    assertEquals(Const.UTC, set.timezone());
    assertEquals(1, set.arraySize());
  }
  
  @Test
  public void closeRelease() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    when(set_d.complete()).thenReturn(true);
    when(set_d.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    PooledObject po = mock(PooledObject.class);
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.setPooledObject(po);
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // final, d
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    set.close();
    assertEquals(-1, set.start().epoch());
    assertEquals(-1, set.end().epoch());
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertEquals(0, set.ndptss.size());
    assertEquals(0, set.last_multi);
    assertTrue(set.timeseries.isEmpty());
    verify(po, times(1)).release();
  }
  
  @Test
  public void handleSingleAlignedOneSeriesComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559998800));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        3600_000,   // 1h
        3600_000,   // 1h
        2,          // num new sets
        1559995200,
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleAlignedTwoSeriesCompleteInitially() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559998800));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    
    long[] sizes = new long[] {
        3600_000,   // 1h
        3600_000,   // 1h
        2,          // num new sets
        1559995200,
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    verify(SERIES.get(1), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleAlignedTwoSeriesCompleteOnSecond() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559998800));
    
    long[] sizes = new long[] {
        3600_000,   // 1h
        3600_000,   // 1h
        2,          // num new sets
        1559995200,
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    verify(SERIES.get(1), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleAlignedNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559998800));
    
    long[] sizes = new long[] {
        3600_000,   // 1h
        3600_000,   // 1h
        2,          // num new sets
        1559995200,
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    assertEquals(0, SERIES.size());
    verify(node, times(1)).sendUpstream(any(NoDataPartialTimeSeries.class));
  }
  
  @Test
  public void handleSingleLargerSegmentOneSeriesComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    // pretend it's a day
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559952000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1560038400));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        3600_000,    // 1h
        3600_000,    // 1h
        24,          // num new sets
        1559995200,  // fudged
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleLargerSegmentTwoSeriesCompleteInitially() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    // pretend it's a day
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559952000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1560038400));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    
    long[] sizes = new long[] {
        3600_000,    // 1h
        3600_000,    // 1h
        24,          // num new sets
        1559995200,  // fudged
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    verify(SERIES.get(1), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleLargerSegmentTwoSeriesCompleteOnSecond() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    // pretend it's a day
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559952000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1560038400));
    
    long[] sizes = new long[] {
        3600_000,    // 1h
        3600_000,    // 1h
        24,          // num new sets
        1559995200,  // fudged
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(1, set.count.get());
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    verify(SERIES.get(1), times(1)).addSeries(pts);
  }
  
  @Test
  public void handleSingleLargerSegmentNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    // pretend it's a day
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559952000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1560038400));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        3600_000,    // 1h
        3600_000,    // 1h
        24,          // num new sets
        1559995200,  // fudged
        1559998800
    };
   
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    assertEquals(0, SERIES.size());
    verify(node, times(1)).sendUpstream(any(NoDataPartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600,  // start at midnight
        1559520000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, d
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    for (int i = 0; i < set.completed_array.length; i++) {
      assertFalse(set.completed_array[i].get());
    }
    assertEquals(1, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderTwoComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, d
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertFalse(set.completed_array[3].get());
    assertEquals(1, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderAllComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    when(set_d.complete()).thenReturn(true);
    when(set_d.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, d
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    for (int i = 0; i < set.completed_array.length; i++) {
      assertTrue(set.completed_array[i].get());
    }
    assertEquals(1, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderStartsWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(0, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(0, SERIES.size());
    assertEquals(2, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    
    // final, d
    PartialTimeSeries pts_d = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderEndsWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // final, d
    PartialTimeSeries pts_d = mock(NoDataPartialTimeSeries.class);
    when(pts_d.set()).thenReturn(set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
    
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderMiddleWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    
    // final, d
    PartialTimeSeries pts_d = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderLostRace() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // ******* OOPS ******
    // now pass in c AGAIN
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(2, set.set_boundaries.get(5)); // now we have two!
    verify(SERIES.get(0), times(1)).addSeries(pts); // grrr, this should never happen
    
    // final, d
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(2, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);

    assertEquals(1, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedInOrderNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(1, set.ndptss.size());
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(2, set.ndptss.size());
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.process(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(3, set.ndptss.size());
    
    // final, d
    when(pts.set()).thenReturn(set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
    
    assertEquals(0, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, times(1)).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // d is at 2 now
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c, shifts d over.
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    for (int i = 0; i < set.completed_array.length; i++) {
      assertFalse(set.completed_array[i].get());
    }
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }

  @Test
  public void handleMultipleAlignedOutOfOrderTwoComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // d is at 2 now
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c, shifts d over.
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertFalse(set.completed_array[3].get());
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderAllComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    when(set_d.complete()).thenReturn(true);
    when(set_d.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // d is at 2 now
    pts = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c, shifts d over.
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // final, a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    for (int i = 0; i < set.completed_array.length; i++) {
      assertTrue(set.completed_array[i].get());
    }
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderStartsWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(0, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // d is at 2 now
    PartialTimeSeries pts_d = mock(NoDataPartialTimeSeries.class);
    when(pts_d.set()).thenReturn(set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(0, SERIES.size());
    assertEquals(2, set.ndptss.size());
    
    // now pass in c, shifts d over.
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // final, a
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderEndsWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // d is at 2 now
    PartialTimeSeries pts_d = mockSeries(NumericLongArrayType.TYPE, set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in c, shifts d over.
    PartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // final, a
    PartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderMiddleWithNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // d is at 2 now
    PartialTimeSeries pts_d = mock(NoDataPartialTimeSeries.class);
    when(pts_d.set()).thenReturn(set_d);
    set.process(pts_d);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in c, shifts d over.
    PartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // final, a
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    verify(SERIES.get(0), times(1)).addSeries(pts_d);
    assertEquals(1, SERIES.size());
    assertEquals(2, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(1, set.ndptss.size());
    
    // d is at 2 now
    when(pts.set()).thenReturn(set_d);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(2, set.ndptss.size());
    
    // now pass in c, shifts d over.
    when(pts.set()).thenReturn(set_c);
    set.process(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(3, set.ndptss.size());
    
    // final, a
    when(pts.set()).thenReturn(set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));

    assertEquals(0, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, times(1)).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrder() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderTwoComplete() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderAllComplete() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderStartsWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(0, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderEndWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c =mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderMiddleWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in b
    PartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(1, set.ndptss.size());
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(2, set.ndptss.size());
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    assertEquals(0, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, times(1)).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrder() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);

    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrderTwoComplete() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);

    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrderAllComplete() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    verify(SERIES.get(0), times(1)).addSeries(pts);

    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrderStartsWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mock(NoDataPartialTimeSeries.class);
    when(pts_b.set()).thenReturn(set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    assertEquals(0, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in a
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());

    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrderEndWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in a
    PartialTimeSeries pts_a = mock(NoDataPartialTimeSeries.class);
    when(pts_a.set()).thenReturn(set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());

    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrderMiddleWithNoData() throws Exception {
    when(query.endTime()).thenReturn(new SecondTimeStamp(1559620800));
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts_b = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts_b);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    assertEquals(1, SERIES.size());
    assertEquals(0, set.ndptss.size());
    
    // now pass in c
    PartialTimeSeries pts_c = mock(NoDataPartialTimeSeries.class);
    when(pts_c.set()).thenReturn(set_c);
    set.process(pts_c);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    
    // now pass in a
    PartialTimeSeries pts_a = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts_a);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());  

    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());

    verify(SERIES.get(0), times(1)).addSeries(pts_a);
    verify(SERIES.get(0), times(1)).addSeries(pts_b);
    verify(SERIES.get(0), times(1)).addSeries(pts_c);
    assertEquals(1, SERIES.size());
    assertEquals(1, set.ndptss.size());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleFirstCompleteThenSecondWith2() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        43200_000,  // 12h
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now we're complete
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_b, 24);
    set.process(pts);

    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertEquals(2, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleFirstWith2ThenSecondComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        43200_000,  // 12h
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    verify(SERIES.get(0), times(1)).addSeries(pts);
    
    // now we're complete
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);

    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertEquals(2, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleBothWith2CompleteOnSecond() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        43200_000,  // 12h
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now we're complete for a but not b
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);

    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now we're complete for b
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_b, 24);
    set.process(pts);
    
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertEquals(2, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleBothWith2Complete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(2);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        43200_000,  // 12h
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now we're complete for a but not b
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);

    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    // now we're complete for b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b, 24);
    set.process(pts);
    
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertEquals(2, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleDisjointTimeSeriesComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(3);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(2);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        64800_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(6, set.set_boundaries.length());
    assertEquals(3, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now pass in b
    pts = mockSeries(NumericLongArrayType.TYPE, set_b, 24);
    set.process(pts);
    
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now pass in c
    pts = mockSeries(NumericLongArrayType.TYPE, set_c, 1);
    set.process(pts);
    
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertFalse(set.completed_array[2].get());
    
    // now set a diff ts for a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 16);
    set.process(pts);
    
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertFalse(set.completed_array[2].get());
    
    // diff and final for b
    pts = mockSeries(NumericLongArrayType.TYPE, set_c, 7);
    set.process(pts);
    
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    
    // last one for a
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 32);
    set.process(pts);
    
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(6, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    assertTrue(set.completed_array[2].get());
    
    assertEquals(6, SERIES.size());
    assertTrue(set.ndptss.isEmpty());
    verify(node, never()).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleTrailingSegmentOneOverlap1Series() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559990700));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    // follows single path
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    assertNull(set.completed_array);
  }
  
  @Test
  public void handleMultipleTrailingSegmentOneOverlap2Series() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559990700));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(1, set.count.get());
    assertNull(set.completed_array);
        
    // next series is done
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertNull(set.set_boundaries);
    assertNull(set.completed_array);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertNull(set.completed_array);
  }

  @Test
  public void handleMultipleTrailingSegmentOneOverlapNoData() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559990700));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    set.process(pts);
    
    // follows the non-multi path
    assertNull(set.set_boundaries);
    assertFalse(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    assertNull(set.completed_array);
    verify(node, times(1)).sendUpstream(any(PartialTimeSeries.class));
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlaps1Series() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlaps1SeriesIgnoredSetAfterComplete() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559998800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlaps1SeriesIgnoredSetBeforeComplete() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559998800));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_c);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlaps1SeriesOutOfOrder() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlaps2SeriesOutOfOrder() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(2);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(2);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mockSeries(NumericLongArrayType.TYPE, set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertFalse(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_a, 24);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mockSeries(NumericLongArrayType.TYPE, set_b, 24);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(2, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
  }
  
  @Test
  public void handleMultipleTrailingSegmentTwoOverlapsNoDataOutOfOrder() throws Exception {
    // e.g. 45 minute downsample produces 3 hour segments and we only want the 
    // the first chunk here.
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559988000));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_a.complete()).thenReturn(true);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559991600));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559995200));
    when(set_b.complete()).thenReturn(true);
    
    long[] sizes = new long[] {
        2700_000,   // 45m
        10800_000,  // 3h
        3,          // num new sets
        1559955600, 
        1559966400,
        1559977200,
        1559988000
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    // just the first 2 45m
    DownsampleConfig config = mock(DownsampleConfig.class);
    when(config.endTime()).thenReturn(new SecondTimeStamp(1559993400));
    when(node.config()).thenReturn(config);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 3);
    
    PartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertFalse(set.completed_array[1].get());
    
    pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    set.process(pts);
    
    assertEquals(4, set.set_boundaries.length());
    assertEquals(2, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    assertTrue(set.completed_array[0].get());
    assertTrue(set.completed_array[1].get());
    verify(node, times(1)).sendUpstream(any(PartialTimeSeries.class));
  }
  
  PartialTimeSeries mockSeries(final TypeToken<?> type, 
                               final PartialTimeSeriesSet set) {
    return mockSeries(type, set, 42);
  }
  
  PartialTimeSeries mockSeries(final TypeToken<?> type, 
                               final PartialTimeSeriesSet set,
                               final long hash) {
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    TimeSeriesDataType data = mock(TimeSeriesDataType.class);
    when(pts.idHash()).thenReturn(hash);
    when(pts.value()).thenReturn(data);
    when(pts.set()).thenReturn(set);
    when(data.type()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return type;
      }
    });
    return pts;
  }

  void debug(final DownsamplePartialTimeSeriesSet set) {
    for (int i = 0; i < set.set_boundaries.length(); i++) {
      if (i % 2 == 0) {
        if (set.set_boundaries.get(i) == 0) {
          System.out.println(i + " null");
        } else {
          System.out.println(i + " " + (set.set_boundaries.get(i) >>> 32) + " => " 
              + (set.set_boundaries.get(i) & DownsamplePartialTimeSeriesSet.END_MASK));
        }
      } else {
        System.out.println(i + " => " + set.set_boundaries.get(i));
      }
    }
  }
}

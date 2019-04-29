package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Maps;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.MockTrace;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.PooledPSTRunnable;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericPartialTimeSeriesPool;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;
import net.opentsdb.uid.UniqueIdType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Scanner.class })
public class TestTsdb1xScanner2 extends UTBase {
  private Tsdb1xScanners2 owner;
  private Tsdb1xHBaseQueryNode node;
  private Tsdb1xQueryResult results;
  private Schema schema; 
  private QueryContext context;
  private QueryPipelineContext ctx;
  private TimeSeriesDataSourceConfig config;
  private Map<Long, Tsdb1xPartialTimeSeriesSet> sets;
  
  @Before
  public void before() throws Exception {
    results = mock(Tsdb1xQueryResult.class);
    node = mock(Tsdb1xHBaseQueryNode.class);
    owner = mock(Tsdb1xScanners2.class);
    schema = spy(new Schema(schema_factory, tsdb, null));
    config = mock(TimeSeriesDataSourceConfig.class);
    sets = Maps.newHashMap();
    
    when(owner.node()).thenReturn(node);
    when(node.config()).thenReturn(config);
    when(node.fetchDataType(any(byte.class))).thenReturn(true);
    when(node.schema()).thenReturn(schema);
    
    context = mock(QueryContext.class);
    when(context.mode()).thenReturn(QueryMode.SINGLE);
    QueryPipelineContext qpc = mock(QueryPipelineContext.class);
    when(qpc.queryContext()).thenReturn(context);
    when(node.pipelineContext()).thenReturn(qpc);
    
    ctx = mock(QueryPipelineContext.class);
    when(ctx.tsdb()).thenReturn(tsdb);
    when(node.pipelineContext()).thenReturn(ctx);
    when(ctx.queryContext()).thenReturn(context);

    when(results.resultIsFullErrorMessage()).thenReturn("Boo!");
    when(owner.getSet(any(TimeStamp.class))).thenAnswer(
        new Answer<Tsdb1xPartialTimeSeriesSet>() {

      @Override
      public Tsdb1xPartialTimeSeriesSet answer(InvocationOnMock invocation)
          throws Throwable {
        final TimeStamp ts = (TimeStamp) invocation.getArguments()[0];
        Tsdb1xPartialTimeSeriesSet set = sets.get(ts.epoch());
        if (set == null) {
          set = new Tsdb1xPartialTimeSeriesSet();
          sets.put(ts.epoch(), set);
        }
        return set;
      }
      
    });
    Tsdb1xNumericPartialTimeSeriesPool pts_allocator = new Tsdb1xNumericPartialTimeSeriesPool();
    ObjectPool pts_pool = mock(ObjectPool.class);
    when(pts_pool.claim()).thenAnswer(new Answer<PooledObject>() {

      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        PooledObject obj = mock(PooledObject.class);
        Tsdb1xPartialTimeSeries pts = (Tsdb1xPartialTimeSeries) pts_allocator.allocate();
        when(obj.object()).thenReturn(pts);
        return obj;
      }
      
    });
    when(tsdb.registry.getObjectPool(Tsdb1xNumericPartialTimeSeriesPool.TYPE))
      .thenReturn(pts_pool);
    
    ObjectPool array_pool = mock(ObjectPool.class);
    LongArrayPool array_allocator = new LongArrayPool();
    when(array_pool.claim()).thenAnswer(new Answer<PooledObject>() {

      @Override
      public PooledObject answer(InvocationOnMock invocation) throws Throwable {
        PooledObject obj = mock(PooledObject.class);
        long[] array = (long[]) array_allocator.allocate();
        when(obj.object()).thenReturn(array);
        return obj;
      }
      
    });
    when(tsdb.registry.getObjectPool(LongArrayPool.TYPE))
      .thenReturn(array_pool);
//    
//    ExecutorService service = mock(ExecutorService.class);
//    when(tsdb.getQueryThreadPool()).thenReturn(service);
//    when(service.submit(any(Runnable.class))).thenAnswer(new Answer<Future<?>>() {
//
//      @Override
//      public Future<?> answer(InvocationOnMock invocation) throws Throwable {
//        ((Runnable) invocation.getArguments()[0]).run();
//        return null;
//      }
//      
//    });
  }
  
  @Test
  public void foo() throws Exception {
//    final QueryFilter filter = TagValueRegexFilter.newBuilder()
//            .setFilter(TAGV_B_STRING + ".*")
//            .setTagKey(TAGK_STRING)
//            .build();
    when(owner.filterDuringScan()).thenReturn(false);
    when(config.getFilter()).thenReturn(null);
    
    Scanner hbase_scanner = metricStartStopScanner(Series.DOUBLE_SERIES, METRIC_BYTES);
    Tsdb1xScanner2 scanner = new Tsdb1xScanner2(owner, hbase_scanner, 0, null);
    trace = new MockTrace(true);
    
    scanner.fetchNext(results, trace.newSpan("UT").start());
    
    System.out.println(tsdb.runnables.size());
    for (final Runnable runnable : tsdb.runnables) {
      System.out.println(((PooledPSTRunnable) runnable).pts.set().complete() + "  " 
          + ((PooledPSTRunnable) runnable).pts.set().totalSets() + "  " + 
          ((PooledPSTRunnable) runnable).pts.set().timeSeriesCount());
    }
    verify(hbase_scanner, times(2)).nextRows();
    verify(hbase_scanner, times(1)).close();
//    verify(results, times(TS_DOUBLE_SERIES_COUNT)).decode(
//        any(ArrayList.class), any(RollupInterval.class));
//    verify(owner, times(1)).scannerDone();
//    verify(owner, never()).exception(any(Throwable.class));
//    assertEquals(0, scanner.keepers.size());
//    assertEquals(0, scanner.skips.size());
//    assertTrue(scanner.keys_to_ids.isEmpty());
//    assertEquals(State.COMPLETE, scanner.state());
//    assertNull(scanner.buffer());
//    verify(schema, times(2)).getName(eq(UniqueIdType.METRIC), 
//        eq(METRIC_BYTES), any(Span.class));
//    
//    assertEquals(17, trace.spans.size());
//    assertEquals("net.opentsdb.storage.Tsdb1xScanner$ScannerCB_0", 
//        trace.spans.get(16).id);
//    assertEquals("OK", trace.spans.get(16).tags.get("status"));
  }
  
  Scanner metricStartStopScanner(final Series series, final byte[] metric) {
    final Scanner scanner = client.newScanner(DATA_TABLE);
    switch (series) {
    case SINGLE_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_SINGLE_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_SINGLE_SERIES + (TS_SINGLE_SERIES_COUNT * TS_SINGLE_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case DOUBLE_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_DOUBLE_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_DOUBLE_SERIES + (TS_DOUBLE_SERIES_COUNT * TS_DOUBLE_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case MULTI_SERIES_EX:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_MULTI_SERIES_EX, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_MULTI_SERIES_EX + (TS_MULTI_SERIES_EX_COUNT * TS_MULTI_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    case NSUI_SERIES:
      scanner.setStartKey(makeRowKey(
          metric, 
          TS_NSUI_SERIES, 
          (byte[][]) null));
      scanner.setStopKey(makeRowKey(METRIC_BYTES, 
          TS_NSUI_SERIES + (TS_NSUI_SERIES_COUNT * TS_NSUI_SERIES_INTERVAL), 
          (byte[][]) null));
      break;
    default:
      throw new RuntimeException("YO! Implement me: " + series);
    }
    return scanner;
  }
}

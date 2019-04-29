package net.opentsdb.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.rollup.DefaultRollupConfig;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Scanner.class, Tsdb1xScanners.class, 
  Tsdb1xScanner.class })
public class TestTsdb1xScanners2 extends UTBase {

  private Tsdb1xHBaseQueryNode node;
  private TimeSeriesDataSourceConfig source_config;
  private DefaultRollupConfig rollup_config;
  private QueryPipelineContext context;
  private SemanticQuery query;
  
  @Before
  public void before() throws Exception {
    node = mock(Tsdb1xHBaseQueryNode.class);
    when(node.schema()).thenReturn(schema);
    when(node.parent()).thenReturn(data_store);
    rollup_config = mock(DefaultRollupConfig.class);
    when(schema.rollupConfig()).thenReturn(rollup_config);
    
    PowerMockito.whenNew(Tsdb1xScanner.class).withAnyArguments()
      .thenAnswer(new Answer<Tsdb1xScanner>() {
        @Override
        public Tsdb1xScanner answer(InvocationOnMock invocation)
            throws Throwable {
          return mock(Tsdb1xScanner.class);
        }
      });
    
    query = SemanticQuery.newBuilder()
        .setMode(QueryMode.SINGLE)
        .setStart(Integer.toString(START_TS))
        .setEnd(Integer.toString(END_TS))
        .setExecutionGraph(Collections.emptyList())
        .build();
    
    source_config = (TimeSeriesDataSourceConfig) 
        DefaultTimeSeriesDataSourceConfig.newBuilder()
        .setMetric(MetricLiteralFilter.newBuilder()
            .setMetric(METRIC_STRING)
            .build())
        .setId("m1")
        .build();
    
    when(data_store.dynamicString(Tsdb1xHBaseDataStore.ROLLUP_USAGE_KEY)).thenReturn("Rollup_Fallback");
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.EXPANSION_LIMIT_KEY)).thenReturn(4096);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.ROWS_PER_SCAN_KEY)).thenReturn(1024);
    when(data_store.dynamicInt(Tsdb1xHBaseDataStore.MAX_MG_CARDINALITY_KEY)).thenReturn(4096);
    
    when(rollup_config.getIdForAggregator("sum")).thenReturn(1);
    when(rollup_config.getIdForAggregator("count")).thenReturn(2);
    
    context = mock(QueryPipelineContext.class);
    when(node.pipelineContext()).thenReturn(context);
    when(context.query()).thenReturn(query);
    when(context.upstreamOfType(any(QueryNode.class), any()))
      .thenReturn(Collections.emptyList());
  }
  
  
  
}

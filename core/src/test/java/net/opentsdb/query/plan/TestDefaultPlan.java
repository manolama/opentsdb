package net.opentsdb.query.plan;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryPipelineContext;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryDataSourceFactory;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

public class TestDefaultPlan {

  private MockTSDB tsdb;
  private QueryContext query_context;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    tsdb.registry = spy(new DefaultRegistry(tsdb));
    query_context = mock(QueryContext.class);
    try {
      ((DefaultRegistry) tsdb.registry).initialize(true).join();
    } catch (Exception e) { }
    
    TimeSeriesDataStoreFactory factory = mock(TimeSeriesDataStoreFactory.class);
    when(factory.supportsPushdown(GroupByConfig.class)).thenReturn(true);
    when(factory.supportsPushdown(DownsampleConfig.class)).thenReturn(true);
    
    QueryDataSourceFactory source_factory = mock(QueryDataSourceFactory.class);
    when(source_factory.getFactory()).thenReturn(factory);
    when(source_factory.newNode(any(QueryPipelineContext.class), anyString(), any(QueryNodeConfig.class)))
      .thenReturn(mock(QueryNode.class));
    
    when(tsdb.registry.getQueryNodeFactory("datasource"))
      .thenReturn((QueryNodeFactory) source_factory);
  }
  
  @Test
  public void voo() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("voo")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("m1")
                .setType("DataSource")
                .setConfig(QuerySourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.cpu.user")
                        .build())
                    .setId("m1")
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("downsample")
                .setSources(Lists.newArrayList("m1"))
                .setConfig(DownsampleConfig.newBuilder()
                    .setId("downsample")
                    .setAggregator("sum")
                    .setInterval("1m")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("groupby")
                .setSources(Lists.newArrayList("downsample"))
                .setConfig(GroupByConfig.newBuilder()
                    .setAggregator("sum")
                    .setGroupAll(true)
                    .setId("groupby")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("expression")
                .setSources(Lists.newArrayList("groupby"))
                .setConfig(ExpressionConfig.newBuilder()
                    .setAs("e1")
                    .setExpression("m1 + 42")
                    .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                        .setType(JoinType.NATURAL)
                        .build())
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .setId("expression")
                    .build())
                .build())
            
            .build())
        .build();
    
    MockPipeline context = new MockPipeline(tsdb, query, 
        query_context, Lists.newArrayList(mock(QuerySink.class)));
    context.initialize(null);
    
    System.out.println(context.plan().base_config_graph);
  }
  
  @Test
  public void voo2() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("voo")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("m1")
                .setType("DataSource")
                .setConfig(QuerySourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.cpu.user")
                        .build())
                    .setId("m1")
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("downsample")
                .setSources(Lists.newArrayList("expression"))
                .setConfig(DownsampleConfig.newBuilder()
                    .setId("downsample")
                    .setAggregator("sum")
                    .setInterval("1m")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("groupby")
                .setSources(Lists.newArrayList("downsample"))
                .setConfig(GroupByConfig.newBuilder()
                    .setAggregator("sum")
                    .setGroupAll(true)
                    .setId("groupby")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("expression")
                .setSources(Lists.newArrayList("m1"))
                .setConfig(ExpressionConfig.newBuilder()
                    .setAs("e1")
                    .setExpression("m1 + 42")
                    .setJoinConfig((JoinConfig) JoinConfig.newBuilder()
                        .setType(JoinType.NATURAL)
                        .build())
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .build())
        .build();
    
    TimeSeriesDataStoreFactory factory = mock(TimeSeriesDataStoreFactory.class);
    when(factory.supportsPushdown(GroupByConfig.class)).thenReturn(true);
    when(factory.supportsPushdown(DownsampleConfig.class)).thenReturn(true);
    
    QueryDataSourceFactory source_factory = mock(QueryDataSourceFactory.class);
    when(source_factory.getFactory()).thenReturn(factory);
    when(source_factory.newNode(any(QueryPipelineContext.class), anyString(), any(QueryNodeConfig.class)))
      .thenReturn(mock(QueryNode.class));
    
    when(tsdb.registry.getQueryNodeFactory("datasource"))
      .thenReturn((QueryNodeFactory) source_factory);
    QueryPipelineContext context = mock(QueryPipelineContext.class);
    when(context.tsdb()).thenReturn(tsdb);
    when(context.query()).thenReturn(query);
    
    DefaultPlan plan = new DefaultPlan(context, mock(QueryNode.class));
    plan.plan();
    
    System.out.println(plan.base_config_graph);
  }
  
  @Test
  public void voo3() throws Exception {
    SemanticQuery query = SemanticQuery.newBuilder()
        .setStart("1h-ago")
        .setMode(QueryMode.SINGLE)
        .setExecutionGraph(ExecutionGraph.newBuilder()
            .setId("voo")
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("m1")
                .setType("DataSource")
                .setConfig(QuerySourceConfig.newBuilder()
                    .setMetric(MetricLiteralFilter.newBuilder()
                        .setMetric("sys.cpu.user")
                        .build())
                    .setId("m1")
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("downsample")
                .setSources(Lists.newArrayList("m1"))
                .setConfig(DownsampleConfig.newBuilder()
                    .setId("downsample")
                    .setAggregator("sum")
                    .setInterval("1m")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
            
            .addNode(ExecutionGraphNode.newBuilder()
                .setId("groupby")
                .setSources(Lists.newArrayList("downsample"))
                .setConfig(GroupByConfig.newBuilder()
                    .setAggregator("sum")
                    .setGroupAll(true)
                    .setId("groupby")
                    .addInterpolatorConfig(NumericInterpolatorConfig.newBuilder()
                        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                        .setDataType(NumericType.TYPE.toString())
                        .build())
                    .build())
                .build())
           
            .build())
        .build();
    
    MockPipeline context = new MockPipeline(tsdb, query, 
        query_context, Lists.newArrayList(mock(QuerySink.class)));
    context.initialize(null);
    
    System.out.println(context.plan().base_config_graph);
  }
  
  class MockPipeline extends AbstractQueryPipelineContext {

    public MockPipeline(TSDB tsdb, TimeSeriesQuery query, QueryContext context,
        Collection<QuerySink> sinks) {
      super(tsdb, query, context, sinks);
      // TODO Auto-generated constructor stub
    }

    @Override
    public void initialize(Span span) {
      // TODO Auto-generated method stub
      initializeGraph(span);
    }

    @Override
    public String id() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}

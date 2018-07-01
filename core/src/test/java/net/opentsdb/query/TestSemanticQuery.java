package net.opentsdb.query;

import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.processor.expressions.ExpressionConfig;

public class TestSemanticQuery {

  private MockTSDB tsdb;
  
  @Before
  public void before() throws Exception {
    tsdb = new MockTSDB();
    tsdb.registry = new DefaultRegistry(tsdb);
    tsdb.config.override(DefaultRegistry.PLUGIN_CONFIG_KEY, "{\"loadDefaultInstances\":false,\"configs\":["
        + "{\"type\":\"net.opentsdb.storage.TimeSeriesDataStoreFactory\",\"plugin\":\"net.opentsdb.storage.MockDataStoreFactory\",\"isDefault\":true}]}");
    try {
      tsdb.registry.initialize(true).join();
    } catch (Exception e) { }
  }
  
  @Test
  public void parse() throws Exception {
    Expression.JEXL_ENGINE.createScript("a.b + ");
  }
  
  @Test
  public void foo() throws Exception {
    System.out.println("Hi");
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("b")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric("b")
                .setStart("1h-ago")
                .build())
            )
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("a")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric("a")
                .setStart("1h-ago")
                .build())
            )
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("e1")
            .setType("Expression")
            .setConfig(new ExpressionConfig.Builder()
                .setExpression("(sys.'if'.out) * (sys.'if'.in)")
                .setId("e1")
                .build()))
        .build();
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setExecutionGraph(graph)
        .addSink(mock(QuerySink.class))
        .build();
    
    SemanticQueryContext ctx = (SemanticQueryContext) new SemanticQueryContext.Builder()
        .setTSDB(tsdb)
        .setMode(QueryMode.SINGLE)
        .setQuery(query)
        .build();
  }
}

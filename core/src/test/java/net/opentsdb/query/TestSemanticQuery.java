package net.opentsdb.query;

import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.joins.JoinConfig;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.utils.Pair;

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
    
    JoinConfig jc = new JoinConfig(JoinType.INNER, Lists.newArrayList(new Pair<String, String>("host", "host")));
    
    ExecutionGraph graph = ExecutionGraph.newBuilder()
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("sys.if.out")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric("sys.if.out")
                .setStart("1h-ago")
                .build())
            )
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("sys.if.in")
            .setType("DataSource")
            .setConfig(QuerySourceConfig.newBuilder()
                .setMetric("sys.if.in")
                .setStart("1h-ago")
                .build())
            )
        .addNode(ExecutionGraphNode.newBuilder()
            .setId("e1")
            .setType("Expression")
            .setConfig(new ExpressionConfig.Builder()
                .setExpression("(sys.'if'.out) * (sys.'if'.in)")
                .setJoinConfig(jc)
                .setId("e1")
                .build()))
        .build();
    
    class LocalSink implements QuerySink {

      @Override
      public void onComplete() {
        System.out.println("ALL Done.");
      }

      @Override
      public void onNext(QueryResult next) {
        System.out.println("RESULT: " + next);
      }

      @Override
      public void onError(Throwable t) {
        System.out.println("Exception!");
        t.printStackTrace();
      }
      
    }
    
    SemanticQuery query = SemanticQuery.newBuilder()
        .setExecutionGraph(graph)
        .addSink(new LocalSink())
        .build();
    
    SemanticQueryContext ctx = (SemanticQueryContext) new SemanticQueryContext.Builder()
        .setTSDB(tsdb)
        .setMode(QueryMode.SINGLE)
        .setQuery(query)
        .build();
    
    ctx.fetchNext(null);
  }
}

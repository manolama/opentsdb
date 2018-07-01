package net.opentsdb.query.processor.expressions;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.query.MultiQueryNodeFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.BranchType;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.ExpNodeConfig;

public class ExpressionFactory implements MultiQueryNodeFactory, TSDBPlugin {
  private final Logger LOG = LoggerFactory.getLogger(ExpressionFactory.class);
  
  public ExpressionFactory() {
  
  }

  @Override
  public Collection<QueryNode> newNodes(QueryPipelineContext context, String id,
      QueryNodeConfig config, List<ExecutionGraphNode> nodes) {
    
    final ExpressionConfig c = (ExpressionConfig) config;
    
    List<ExpNodeConfig> configs = new ExpressionNodeBuilder().parse(c.expression, id);
    List<QueryNode> query_nodes = Lists.newArrayListWithExpectedSize(configs.size());
    for (final ExpNodeConfig enc : configs) {
      BinaryExpressionNode node = new BinaryExpressionNode(this, context, enc.getId(), 
          (ExpressionConfig) config, enc);
      query_nodes.add(node);
      
      ExecutionGraphNode.Builder builder = ExecutionGraphNode.newBuilder()
          .setConfig(enc)
          .setId(enc.getId());
      if (enc.left_type == BranchType.SUB_EXP || enc.left_type == BranchType.VARIABLE) {
        builder.addSource(enc.left);
      }
      if (enc.right_type == BranchType.SUB_EXP || enc.right_type == BranchType.VARIABLE) {
        builder.addSource(enc.right);
      }
      nodes.add(builder.build());
    }
    return query_nodes;
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return ExpressionConfig.class;
  }

  @Override
  public String id() {
    return "expression";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    // TODO Auto-generated method stub
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    // TODO Auto-generated method stub
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

}

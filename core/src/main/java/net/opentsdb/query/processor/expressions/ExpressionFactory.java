package net.opentsdb.query.processor.expressions;

import java.util.Collection;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

public class ExpressionFactory extends BaseQueryNodeFactory {

  public ExpressionFactory(String id) {
    super(id);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Collection<QueryNode> newNodes(QueryPipelineContext context,
      String id) {
    throw new UnsupportedOperationException("Config is required.");
  }

  @Override
  public Collection<QueryNode> newNodes(QueryPipelineContext context, String id,
      QueryNodeConfig config) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Class<? extends QueryNodeConfig> nodeConfigClass() {
    return ExpressionConfig.class;
  }

}

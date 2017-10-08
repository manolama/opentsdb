package net.opentsdb.query.processor;

import java.util.Map;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.pojo.Expression;

public class JexlExpressionFactory implements QueryNodeFactory {

  @Override
  public QueryNode newNode(QueryPipelineContext context,
      QueryNodeConfig config) {
    return new JexlExpression(context, (JEConfig) config);
  }

  @Override
  public String id() {
    return "JexlExpression";
  }

  public static class JEConfig implements QueryNodeConfig {
    public Expression exp;
    public Map<String, String> ids_map; // id().metric() to varible ID
    
    @Override
    public String id() {
      return exp.getId();
    }
  }
}

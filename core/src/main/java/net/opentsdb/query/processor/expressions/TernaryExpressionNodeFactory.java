package net.opentsdb.query.processor.expressions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

public class TernaryExpressionNodeFactory extends BaseQueryNodeFactory {
  public static final String TYPE = "TernaryExpression";
  
  @Override
  public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
      JsonNode node) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setupGraph(QueryPipelineContext context, QueryNodeConfig config,
      QueryPlanner planner) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context,
      QueryNodeConfig config) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String type() {
    // TODO Auto-generated method stub
    return null;
  }

}

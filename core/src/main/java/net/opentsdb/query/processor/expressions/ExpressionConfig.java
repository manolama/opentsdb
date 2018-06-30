package net.opentsdb.query.processor.expressions;

import java.util.Map;

import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;

import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.joins.JoinConfig;

public class ExpressionConfig extends BaseQueryNodeConfigWithInterpolators {
  public String expression;
  public JoinConfig joinConfig;
  public Map<String, Map<TypeToken<?>, QueryInterpolatorConfig>> variable_interp_configs;
  
  protected ExpressionConfig(Builder builder) {
    super(builder);
    expression = builder.expression;
    joinConfig = builder.joinConfig;
    variable_interp_configs = builder.variable_interp_configs;
  }

  @Override
  public HashCode buildHashCode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(QueryNodeConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int hashCode() {
    // TODO Auto-generated method stub
    return 0;
  }

  public static class Builder extends BaseQueryNodeConfigWithInterpolators.Builder {

    private String expression;
    private JoinConfig joinConfig;
    private Map<String, Map<TypeToken<?>, QueryInterpolatorConfig>> variable_interp_configs;
    
    @Override
    public QueryNodeConfig build() {
      return new ExpressionConfig(this);
    }
    
  }
}

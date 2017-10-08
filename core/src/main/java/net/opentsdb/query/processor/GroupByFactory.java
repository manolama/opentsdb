package net.opentsdb.query.processor;

import java.util.Set;

import com.google.common.collect.Sets;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;

public class GroupByFactory implements QueryNodeFactory {

  @Override
  public QueryNode newNode(QueryPipelineContext context,
                           QueryNodeConfig config) {
    return new GroupBy(context, (GBConfig) config);
  }

  @Override
  public String id() {
    return "GroupBy";
  }

  public static class GBConfig implements QueryNodeConfig {
    public String id;
    public Set<String> tag_keys = Sets.newHashSet();
    
    @Override
    public String id() {
      return id;
    }
    
  }
}

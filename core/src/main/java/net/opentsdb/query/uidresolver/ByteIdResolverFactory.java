package net.opentsdb.query.uidresolver;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

public class ByteIdResolverFactory extends BaseQueryNodeFactory {

  public ByteIdResolverFactory(String id) {
    super(id);
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context,
                           QueryNodeConfig config) {
    // TODO Auto-generated method stub
    return new ByteIdResolver(this, context);
  }

}

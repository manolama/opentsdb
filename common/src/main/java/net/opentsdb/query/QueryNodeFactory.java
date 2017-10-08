package net.opentsdb.query;

public interface QueryNodeFactory {

  public QueryNode newNode(final QueryPipelineContext context, 
                           final QueryNodeConfig config);
  
  public String id();
  
}

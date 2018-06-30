package net.opentsdb.query;

public interface SingleQueryNodeFactory extends QueryNodeFactory {

  /**
   * Instantiates a new node using the given context and the default
   * configuration for this node.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id);
  
  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config);
  
}

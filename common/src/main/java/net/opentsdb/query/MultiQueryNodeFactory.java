package net.opentsdb.query;

import java.util.Collection;
import java.util.List;

import net.opentsdb.query.execution.graph.ExecutionGraphNode;

public interface MultiQueryNodeFactory extends QueryNodeFactory {

  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param id An ID for this node.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public Collection<QueryNode> newNodes(final QueryPipelineContext context, 
                                        final String id,
                                        final QueryNodeConfig config,
                                        final List<ExecutionGraphNode> nodes);
  
}

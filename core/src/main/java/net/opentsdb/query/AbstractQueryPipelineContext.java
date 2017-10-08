package net.opentsdb.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class AbstractQueryPipelineContext implements QueryPipelineContext {

  protected TimeSeriesQuery query;
  protected QueryContext context;
  protected DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  protected Map<String, QueryNode> vertices;
  protected Set<QueryListener> sinks;
  protected Set<QueryNode> roots;
  
  public AbstractQueryPipelineContext(TimeSeriesQuery original_query, 
                                      QueryContext context) {
    this.query = original_query;
    this.context = context;
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    vertices = Maps.newHashMap();
    sinks = Sets.newHashSet();
    roots = Sets.newHashSet();
  }
  
  @Override
  public QueryContext getContext() {
    return context;
  }
  
  @Override
  public Collection<QueryListener> upstream(final QueryNode node) {
    System.out.println("fetching upstream of: " + node);
    Set<DefaultEdge> upstream = graph.incomingEdgesOf(node);
    if (upstream.isEmpty()) {
      System.out.println("  roots, so returning sinks");
      return sinks;
    }
    List<QueryListener> listeners = Lists.newArrayListWithCapacity(upstream.size());
    for (final DefaultEdge e : upstream) {
      listeners.add(graph.getEdgeSource(e));
    }
    System.out.println("Listeners: " + listeners);
    return listeners;
  }
  
  @Override
  public Collection<QueryNode> downstream(final QueryNode node) {
    Set<DefaultEdge> downstream = graph.outgoingEdgesOf(node);
    if (downstream.isEmpty()) {
      return Collections.emptyList();
    }
    List<QueryNode> downstreams = Lists.newArrayListWithCapacity(downstream.size());
    for (final DefaultEdge e : downstream) {
      downstreams.add(graph.getEdgeTarget(e));
    }
    return downstreams;
  }
  
  @Override
  public Collection<QueryListener> sinks() {
    return sinks;
  }
  
  @Override
  public Collection<QueryNode> roots() {
    return roots;
  }
  
  void initializeNodes() {
    final DepthFirstIterator<QueryNode, DefaultEdge> df_iterator = 
      new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
    while (df_iterator.hasNext()) {
      QueryNode node = df_iterator.next();
      node.initialize();
      if (graph.incomingEdgesOf(node).isEmpty()) {
        roots.add(node);
      }
    }
  }
}

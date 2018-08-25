package net.opentsdb.query.plan;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.DepthFirstIterator;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.MultiQueryNodeFactory;
import net.opentsdb.query.QueryDataSourceFactory;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SingleQueryNodeFactory;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;
import net.opentsdb.utils.Pair;

/**
 * TODO - sources for the pushdowns. Though maybe I don't care about it.
 * 
 * 
 *
 */
public class DefaultPlan {

  private SemanticQuery query;
  private TSDB tsdb;
  private List<QueryNode> roots;
  private QueryNode sink;
  
  DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> base_config_graph;
  
  DefaultPlan(final TSDB tsdb, final SemanticQuery query,
      final QueryNode sink) {
    this.tsdb = tsdb;
    this.query = query;
    roots = Lists.newArrayList();
    this.sink = sink;
  }
  
  DirectedAcyclicGraph<QueryNode, DefaultEdge> plan(final QueryPipelineContext context) {
    final Map<String, ExecutionGraphNode> config_map = 
        Maps.newHashMapWithExpectedSize(query.getExecutionGraph().getNodes().size());
    
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> config_graph = new 
        DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    for (final ExecutionGraphNode node : query.getExecutionGraph().getNodes()) {
      if (config_map.putIfAbsent(node.getId(), node) != null) {
        throw new IllegalArgumentException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.");
      }
      
      config_graph.addVertex(node);
    }
    
    // now link em
    for (final ExecutionGraphNode node : query.getExecutionGraph().getNodes()) {
      if (node.getSources() != null) {
        for (final String source : node.getSources()) {
          try {
            config_graph.addDagEdge(node, config_map.get(source));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to add node: " 
                + node, e);
          } catch (CycleFoundException e) {
            throw new IllegalArgumentException("A cycle was detected "
                + "adding node: " + node, e);
          }
        }
      }
    }
    
    // we'll mutate this shallow clone.
    base_config_graph = (DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>) config_graph.clone();
    
    // now we walk and expand/optimize
    DepthFirstIterator<ExecutionGraphNode, DefaultEdge> iterator = 
        new DepthFirstIterator<ExecutionGraphNode, DefaultEdge>(config_graph);
    while (iterator.hasNext()) {
      final ExecutionGraphNode node = iterator.next();
      
      final Set<DefaultEdge> incoming = config_graph.incomingEdgesOf(node);
      if (incoming.isEmpty()) {
        continue;
      }
      
      QueryNodeConfig config = node.getConfig() != null ? node.getConfig() :
        query.getExecutionGraph().nodeConfigs().get(node.getId());
      
      if (config instanceof QuerySourceConfig) {
        final QueryNodeFactory factory;
        if (!Strings.isNullOrEmpty(node.getType())) {
          factory = tsdb.getRegistry().getQueryNodeFactory(node.getType().toLowerCase());
          System.out.println("  DS: " + node.getType().toLowerCase());
        } else {
          factory = tsdb.getRegistry().getQueryNodeFactory(node.getId().toLowerCase());
          System.out.println("  DS: " + node.getId().toLowerCase());
        }
        
        // TODO - cleanup the source factories. ugg!!!
        if (factory == null || !(factory instanceof QueryDataSourceFactory)) {
          throw new IllegalArgumentException("No node factory found for "
              + "configuration " + node);
        }
        
        List<ExecutionGraphNode> push_downs = Lists.newArrayList();
        
        for (final DefaultEdge edge : incoming) {
          final ExecutionGraphNode n = base_config_graph.getEdgeSource(edge);
          DefaultEdge e = pushDown(node, node, 
              ((QueryDataSourceFactory) factory).getFactory(), n, push_downs);
          if (e != null) {
            base_config_graph.removeEdge(e);
          }
          if (base_config_graph.outgoingEdgesOf(n).isEmpty()) {
            if (base_config_graph.removeVertex(n)) {
              push_downs.add(n);
            }
          }
        }
        
        if (!push_downs.isEmpty()) {
          push_downs.add(node);
        }
        System.out.println("PUSH DOWNS: " + push_downs);
      }
    }
    
    DirectedAcyclicGraph<QueryNode, DefaultEdge> graph = 
        new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    graph.addVertex(sink);
    
    List<Long> initialized = Lists.newArrayList();
    BreadthFirstIterator<ExecutionGraphNode, DefaultEdge> bfi = 
        new BreadthFirstIterator<ExecutionGraphNode, DefaultEdge>(base_config_graph);
    Map<String, QueryNode> nodes_map = Maps.newHashMap();
    while (bfi.hasNext()) {
      final ExecutionGraphNode node = bfi.next();
      if (base_config_graph.incomingEdgesOf(node).isEmpty()) {
        buildNodeGraph(context, graph, node, initialized, nodes_map);
      }
    }
    
    // depth first initiation of the executors since we have to init
    // the ones without any downstream dependencies first.
    final DepthFirstIterator<QueryNode, DefaultEdge> node_iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
    final Set<TimeSeriesDataSource> source_set = Sets.newHashSet();
    while (node_iterator.hasNext()) {
      final QueryNode node = node_iterator.next();
      if (node == sink) {
        continue;
      }
      
      final Set<DefaultEdge> incoming = graph.incomingEdgesOf(node);
      if (incoming.size() == 0 && node != this) {
        try {
          graph.addDagEdge(sink, node);
        } catch (CycleFoundException e) {
          throw new IllegalArgumentException(
              "Invalid graph configuration", e);
        }
        roots.add(node);
      }
      
      if (node instanceof TimeSeriesDataSource) {
        source_set.add((TimeSeriesDataSource) node);
      }
      
      if (node != this) {
        node.initialize(null /* TODO */);
      }
    }
    //sources.addAll(source_set);
    
    return graph;
  }
  
  DefaultEdge pushDown(
      final ExecutionGraphNode parent,
      final ExecutionGraphNode source, 
      TimeSeriesDataStoreFactory factory, 
      ExecutionGraphNode node,
      List<ExecutionGraphNode> push_downs) {
    QueryNodeConfig config = node.getConfig() != null ? node.getConfig() :
      query.getExecutionGraph().nodeConfigs().get(node.getId());
    if (!factory.supportsPushdown(config.getClass())) {
      if (!base_config_graph.containsEdge(node, parent)) {
        try {
          base_config_graph.addDagEdge(node, parent);
        } catch (CycleFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      return null;
    }
    
    // TODO push it down
    final DefaultEdge delete_edge = base_config_graph.getEdge(node, source);
    
    // see if we can walk up for more
    final Set<DefaultEdge> incoming = base_config_graph.incomingEdgesOf(node);
    if (!incoming.isEmpty()) {
      List<DefaultEdge> removals = Lists.newArrayList();
      List<ExecutionGraphNode> nodes = Lists.newArrayList();
      for (final DefaultEdge edge : incoming) {
        final ExecutionGraphNode n = base_config_graph.getEdgeSource(edge);
        nodes.add(n);
        DefaultEdge e = pushDown(parent, node, factory, n, push_downs);
        if (e != null) {
          removals.add(e);
        }
      }
      
      if (!removals.isEmpty()) {
        for (final DefaultEdge e : removals) {
          base_config_graph.removeEdge(e);
        }
      }
      
      for (final ExecutionGraphNode n : nodes) {
        if (base_config_graph.outgoingEdgesOf(n).isEmpty()) {
          if (base_config_graph.removeVertex(n)) {
            push_downs.add(n);
          }
        }
      }
    }
    
    // purge if we pushed everything down
    if (base_config_graph.outgoingEdgesOf(node).isEmpty()) {
      if (base_config_graph.removeVertex(node)) {
        push_downs.add(node);
      }
    }
    
    return delete_edge;
  }

  QueryNode buildNodeGraph(
      QueryPipelineContext context,
      DirectedAcyclicGraph<QueryNode, DefaultEdge> graph, 
      ExecutionGraphNode node, 
      List<Long> initialized,
      Map<String, QueryNode> nodes_map) {
    if (initialized.contains(node.buildHashCode().asLong())) {
      return nodes_map.get(node.getId());
    }
    
    List<QueryNode> sources = Lists.newArrayList();
    for (final DefaultEdge edge : base_config_graph.outgoingEdgesOf(node)) {
      sources.add(buildNodeGraph(context, graph, 
          base_config_graph.getEdgeTarget(edge), initialized, nodes_map));
    }
    
    final QueryNodeFactory factory;
    if (!Strings.isNullOrEmpty(node.getType())) {
      factory = tsdb.getRegistry().getQueryNodeFactory(node.getType().toLowerCase());
    } else {
      factory = tsdb.getRegistry().getQueryNodeFactory(node.getId().toLowerCase());
    }
    if (factory == null) {
      throw new IllegalArgumentException("No node factory found for "
          + "configuration " + node);
    }
    
    QueryNodeConfig node_config = node.getConfig() != null ? 
        node.getConfig() : query.getExecutionGraph().nodeConfigs().get(node.getId());
    if (node_config == null) {
      node_config = query.getExecutionGraph().nodeConfigs().get(node.getType());
    }
    
    QueryNode query_node = null;
    List<ExecutionGraphNode> configs = Lists.newArrayList(node);
    Map<String, QueryNode> map = Maps.newHashMap();
    if (!(factory instanceof SingleQueryNodeFactory)) {
      final Collection<QueryNode> query_nodes = 
          ((MultiQueryNodeFactory) factory).newNodes(
              context, node.getId(), node_config, configs);
      if (query_nodes == null || query_nodes.isEmpty()) {
        throw new IllegalStateException("Factory returned a null or "
            + "empty list of nodes for " + node.getId());
      }
      for (final QueryNode n : query_nodes) {
        if (n == null) {
          throw new IllegalStateException("Factory returned a null "
              + "node for " + node.getId());
        }
        
        map.put(n.id(), n);
        graph.addVertex(n);
      }
      
      for (final ExecutionGraphNode config : configs) {
        if (config.getSources() != null) {
          final QueryNode mnode = map.get(config.getId());
          for (final String source : config.getSources()) {
            final QueryNode snode = map.get(source);
            if (snode != null) {
              try {
                graph.addDagEdge(mnode, snode);
              } catch (CycleFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
              }
            } else {
              // should be in the map now
              for (final QueryNode source_node : sources) {
                try {
                  graph.addDagEdge(mnode, source_node);
                } catch (CycleFoundException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                }
              }
            }
          }
        } else {
          // its the "root" node
          query_node = map.get(config.getId());
        }
      }
      
      initialized.add(node.buildHashCode().asLong());
    } else {
      if (node_config != null) {
        // ugg!!!!
        if (node_config instanceof DownsampleConfig) {
          node_config = DownsampleConfig.newBuilder((DownsampleConfig) node_config)
              .setStart(((SemanticQuery) query).getStart())
              .setEnd(((SemanticQuery) query).getEnd())
              .build();
        }
        query_node = ((SingleQueryNodeFactory) factory)
            .newNode(context, node.getId(), node_config);
      } else {
        query_node = ((SingleQueryNodeFactory) factory)
            .newNode(context, node.getId());
      }
      
      System.out.println("FACTORY: " + factory);
      if (query_node == null) {
        throw new IllegalStateException("Factory returned a null "
            + "instance for " + node);
      }
      
      graph.addVertex(query_node);
    }
    
    nodes_map.put(node.getId(), query_node);
    initialized.add(node.buildHashCode().asLong());
    
    for (final QueryNode source_node : sources) {
      try {
        graph.addDagEdge(query_node, source_node);
      } catch (CycleFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    return query_node;
  }
}

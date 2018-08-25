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

  private final QueryPipelineContext context;
  private List<QueryNode> roots;
  private QueryNode sink;
  private DirectedAcyclicGraph<QueryNode, DefaultEdge> graph;
  List<TimeSeriesDataSource> sources;
  
  DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> base_config_graph;
  
  public DefaultPlan(final QueryPipelineContext context,
      final QueryNode sink) {
    this.context = context;
    roots = Lists.newArrayList();
    this.sink = sink;
    graph = new DirectedAcyclicGraph<QueryNode, DefaultEdge>(DefaultEdge.class);
    sources = Lists.newArrayList();
  }
  
  public void plan() {
    final Map<String, ExecutionGraphNode> config_map = 
        Maps.newHashMapWithExpectedSize(
            context.query().getExecutionGraph().getNodes().size());
    
    DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge> config_graph = new 
        DirectedAcyclicGraph<ExecutionGraphNode, DefaultEdge>(DefaultEdge.class);
    
    for (final ExecutionGraphNode node : 
        context.query().getExecutionGraph().getNodes()) {
      if (config_map.putIfAbsent(node.getId(), node) != null) {
        throw new IllegalArgumentException("The node id \"" 
            + node.getId() + "\" appeared more than once in the "
            + "graph. It must be unique.");
      }
      
      config_graph.addVertex(node);
    }
    
    // now link em
    for (final ExecutionGraphNode node : 
        context.query().getExecutionGraph().getNodes()) {
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
        context.query().getExecutionGraph().nodeConfigs().get(node.getId());
      
      if (config instanceof QuerySourceConfig) {
        final QueryNodeFactory factory;
        if (!Strings.isNullOrEmpty(node.getType())) {
          factory = context.tsdb().getRegistry().getQueryNodeFactory(node.getType().toLowerCase());
          System.out.println("  DS: " + node.getType().toLowerCase());
        } else {
          factory = context.tsdb().getRegistry().getQueryNodeFactory(node.getId().toLowerCase());
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
    
    graph.addVertex(sink);
    System.out.println("ADDED GRAPH SINK: " + sink);
    
    List<Long> initialized = Lists.newArrayList();
    BreadthFirstIterator<ExecutionGraphNode, DefaultEdge> bfi = 
        new BreadthFirstIterator<ExecutionGraphNode, DefaultEdge>(base_config_graph);
    Map<String, QueryNode> nodes_map = Maps.newHashMap();
    nodes_map.put(sink.id(), sink);
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
    sources.addAll(source_set);
  }
  
  DefaultEdge pushDown(
      final ExecutionGraphNode parent,
      final ExecutionGraphNode source, 
      TimeSeriesDataStoreFactory factory, 
      ExecutionGraphNode node,
      List<ExecutionGraphNode> push_downs) {
    final QueryNodeConfig config = node.getConfig() != null ? node.getConfig() :
      context.query().getExecutionGraph().nodeConfigs().get(node.getId());
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
      System.out.println("   ALREADY INITED: " + node.getId());
      return nodes_map.get(node.getId());
    }
    
    List<QueryNode> sources = Lists.newArrayList();
    for (final DefaultEdge edge : base_config_graph.outgoingEdgesOf(node)) {
      System.out.println("   TGT: " + base_config_graph.getEdgeTarget(edge));
      sources.add(buildNodeGraph(context, graph, 
          base_config_graph.getEdgeTarget(edge), initialized, nodes_map));
    }
    
    final QueryNodeFactory factory;
    if (!Strings.isNullOrEmpty(node.getType())) {
      factory = context.tsdb().getRegistry().getQueryNodeFactory(node.getType().toLowerCase());
    } else {
      factory = context.tsdb().getRegistry().getQueryNodeFactory(node.getId().toLowerCase());
    }
    if (factory == null) {
      throw new IllegalArgumentException("No node factory found for "
          + "configuration " + node);
    }
    
    QueryNodeConfig node_config = node.getConfig() != null ? node.getConfig() : 
          context.query().getExecutionGraph().nodeConfigs().get(node.getId());
    if (node_config == null) {
      node_config = context.query().getExecutionGraph().nodeConfigs().get(node.getType());
    }
    
    QueryNode query_node = null;
    List<ExecutionGraphNode> configs = Lists.newArrayList(
        context.query().getExecutionGraph().getNodes());
    Map<String, QueryNode> map = Maps.newHashMap();
    if (!(factory instanceof SingleQueryNodeFactory)) {
      final Collection<QueryNode> query_nodes = 
          ((MultiQueryNodeFactory) factory).newNodes(
              context, node.getId(), node_config, configs);
      if (query_nodes == null || query_nodes.isEmpty()) {
        throw new IllegalStateException("Factory returned a null or "
            + "empty list of nodes for " + node.getId());
      }
      
      Collection<DefaultEdge> tgts = base_config_graph.outgoingEdgesOf(node);
      
      QueryNode last = null;
      for (final QueryNode n : query_nodes) {
        if (n == null) {
          throw new IllegalStateException("Factory returned a null "
              + "node for " + node.getId());
        }
        if (query_node == null) {
          query_node = n;
        }
        last = n;
        
        map.put(n.id(), n);
        graph.addVertex(n);
        nodes_map.put(n.id(), n);
        System.out.println("    PUT NODE: " + n);
      }
      
//      for (final ExecutionGraphNode config : configs) {
//        if (context.query().getExecutionGraph().getNodes().contains(config)) {
//          System.out.println("Skipping: " + config);
//          continue;
//        }
//        
////        if (config.getSources() != null) {
////          QueryNode mnode = map.get(config.getId());
////          if (mnode == null) {
////            mnode = nodes_map.get(config.getId());
////          }
////          
////          System.out.println("WANT MNODE: " + config.getId() + "  => " + mnode);
//////          for (final String source : config.getSources()) {
//////            QueryNode snode = map.get(source);
//////            if (snode != null) {
//////              System.out.println("SNODE: " + snode + "   mNode: " + mnode);
//////              try {
//////                graph.addDagEdge(mnode, snode);
//////              } catch (CycleFoundException e) {
//////                // TODO Auto-generated catch block
//////                e.printStackTrace();
//////              }
//////            } else {
//////              snode = nodes_map.get(source);
//////              if (snode == null) {
//////                // we use the bits above
//////                
//////              }
//////              System.out.println("  NEW SN: " + snode);
////////              // should be in the map now
////////              for (final QueryNode source_node : sources) {
////////                System.out.println("SNODE: " + source_node + "   mNode: " + mnode);
////////                try {
////////                  graph.addDagEdge(mnode, source_node);
////////                } catch (CycleFoundException e) {
////////                  // TODO Auto-generated catch block
////////                  e.printStackTrace();
////////                }
////////              }
//////              System.out.println("NO SOURCE: " + source);
//////            }
//////          }
////        } else {
////          // its the "root" node
////          query_node = map.get(config.getId());
////          System.out.println("ROOT NODE: " + query_node);
////        }
//      }
      
      initialized.add(node.buildHashCode().asLong());
      for (final QueryNode source : sources) {
        try {
          graph.addDagEdge(last, source);
        } catch (CycleFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    } else {
      if (node_config != null) {
        // ugg!!!!
        if (node_config instanceof DownsampleConfig) {
          node_config = DownsampleConfig.newBuilder((DownsampleConfig) node_config)
              .setStart(context.query().getStart())
              .setEnd(context.query().getEnd())
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
      nodes_map.put(query_node.id(), query_node);
      System.out.println("ADDED GRAPH NODE: " + query_node);
    }
    
    initialized.add(node.buildHashCode().asLong());
    
    for (final QueryNode source_node : sources) {
      try {
        System.out.println("QN: " + query_node + "  SN: " + source_node);
        graph.addDagEdge(query_node, source_node);
      } catch (CycleFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    return query_node;
  }

  public List<QueryNode> roots() {
    return roots;
  }
  
  public DirectedAcyclicGraph<QueryNode, DefaultEdge> graph() {
    return graph;
  }
  
  public List<TimeSeriesDataSource> sources() {
    return sources;
  }
}

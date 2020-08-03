package net.opentsdb.query.plan;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.readcache.ReadCacheNodeConfig;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Deferreds;

public class ReadCacheQueryPlanner extends BaseQueryPlanner {

  Set<String> serds;
  CacheGraphNode ctx_cgn;
  
  public ReadCacheQueryPlanner(final QueryPipelineContext context,
      final QueryNode context_sink) {
    super(context, context.query(), context_sink);
    serds = Sets.newHashSet();
    if (query.getSerdesConfigs() != null) {
      for (final SerdesOptions s : query.getSerdesConfigs()) {
        serds.addAll(s.getFilter());
      }
    }
    ctx_cgn = new CacheGraphNode();
    ctx_cgn.node = context_sink_config;
  }
  
  /**
   * Does the hard work.
   */
  public Deferred<Void> plan(final Span span) {
    buildInitialConfigGraph();
    checkSatisfiedFilters();
    findDataSourceNodes();
    
    MutableGraph<CacheGraphNode> cache_graph = GraphBuilder.directed()
        .allowsSelfLoops(false)
        .build();
    cache_graph.addNode(ctx_cgn);
    for (final QueryNodeConfig src : source_nodes) {
      recurse(src, null, src.readCacheable(), cache_graph);
    }
    
    System.out.println(printMe(cache_graph));
    
    // yoink!!
    boolean changed = true;
    while (changed) {
      changed = false;
      Traverser<CacheGraphNode> traverser = Traverser.forGraph(cache_graph);
      CacheGraphNode c = null;
      for (final CacheGraphNode node : traverser.breadthFirst(ctx_cgn)) {
        if (node.cacheable) {
          c = node;
          changed = true;
          break;
        }
      }
      
      if (c != null) {
        System.out.println("   CACHE NODE: " + c.node.getId());
        ReadCacheNodeConfig cfg = ReadCacheNodeConfig.newBuilder()
            .setId(c.node.getId())
            .build();
        Set<QueryNodeConfig> graph = Sets.newHashSet();
        Set<QueryNodeConfig> preds = Sets.newHashSet(config_graph.predecessors(c.node));
        recurseYank(c.node, graph);
        config_graph.addNode(cfg);
        for (final QueryNodeConfig p : preds) {
          config_graph.putEdge(p, cfg);
        }
        recurseRemove(c, cache_graph);
        System.out.println("\n&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n");
        System.out.println(printMe(cache_graph));
      }
    }
    
    System.out.println(this.printConfigGraph());
    
    if (true) {
      return Deferred.fromResult(null);
    }
    
    final List<Deferred<Void>> deferreds = initializeConfigNodes();
    return Deferred.group(deferreds)
        .addCallback(Deferreds.VOID_GROUP_CB)
        .addCallbackDeferring(new ConfigInitCB());
  }
  
  void recurseYank(final QueryNodeConfig c, 
                   Set<QueryNodeConfig> graph) {
    graph.add(c);
    Set<QueryNodeConfig> successors = Sets.newHashSet(config_graph.successors(c));
    for (final QueryNodeConfig s : successors) {
      recurseYank(s, graph);
    }
    config_graph.removeNode(c);
  }
  
  class CacheGraphNode {
    QueryNodeConfig node;
    boolean cacheable;
    
    @Override
    public int hashCode() {
      return node.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof CacheGraphNode)) {
        return false;
      }
      
      return node.equals(((CacheGraphNode) o).node);
    }
  }
  
  void recurseRemove(final CacheGraphNode n, MutableGraph<CacheGraphNode> cache_graph) {
    Set<CacheGraphNode> successors = Sets.newHashSet(cache_graph.successors(n));
    if (source_nodes.contains(n.node)) {
      source_nodes.remove(n.node);
    }
    for (final CacheGraphNode s : successors) {
      recurseRemove(s, cache_graph);
    }
    cache_graph.removeNode(n);
  }
  
  void recurse(final QueryNodeConfig node, 
      final CacheGraphNode successor, 
      boolean cacheable,
      MutableGraph<CacheGraphNode> cache_graph) {
    
    CacheGraphNode cgn = new CacheGraphNode();
    cgn.node = node;
    cgn.cacheable = cacheable && node.readCacheable();
    
    CacheGraphNode extant = null;
    for (final CacheGraphNode n : cache_graph.nodes()) {
      if (n.equals(cgn)) {
        extant = n;
        break;
      }
    }
    
    if (extant == null) {
      cache_graph.addNode(cgn);
      if (successor != null) {
        cache_graph.putEdge(cgn, successor);
      }
      
      if (serds.contains(node.getId())) {
        cache_graph.putEdge(ctx_cgn, cgn);
      }
    }
    
    final boolean next_cacheable;
    if (serds.contains(node.getId())) {
      System.out.println("******* SERDES has: " + node.getId());
      next_cacheable = false;
    } else {      
      next_cacheable = cgn.cacheable; 
    }
    
    for (final QueryNodeConfig pred : config_graph.predecessors(node)) {
      recurse(pred, cgn, next_cacheable, cache_graph);
    }
  }
  
  public String printMe(MutableGraph<CacheGraphNode> cache_graph) {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(" -------------------------\n");
    for (final CacheGraphNode node : cache_graph.nodes()) {
      buffer.append("[V] [" + node.cacheable + "] " + node.node.getId() + " {" + node.getClass().getSimpleName() + "} (" + System.identityHashCode(node) + ")\n");
    }
    buffer.append("\n");
    for (final EndpointPair<CacheGraphNode> pair : cache_graph.edges()) {
      buffer.append("[E] " + pair.nodeU().node.getId() + " (" + System.identityHashCode(pair.nodeU()) + ") => " + pair.nodeV().node.getId() + " (" + System.identityHashCode(pair.nodeV()) + ")\n");
    }
    buffer.append(" -------------------------\n");
    return buffer.toString();
  }
}

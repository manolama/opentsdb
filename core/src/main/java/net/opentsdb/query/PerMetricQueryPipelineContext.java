package net.opentsdb.query;

import java.util.Collection;
import java.util.Set;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.plan.SplitMetricPlanner;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.GroupByFactory;
import net.opentsdb.query.processor.GroupByFactory.GBConfig;
import net.opentsdb.query.processor.JexlExpressionFactory;
import net.opentsdb.query.processor.JexlExpressionFactory.JEConfig;
import net.opentsdb.storage.MockDataStore;
import net.opentsdb.storage.MockDataStore.MDSConfig;

public class PerMetricQueryPipelineContext extends AbstractQueryPipelineContext {

  net.opentsdb.query.pojo.TimeSeriesQuery plan;
  int parallel_id = 0;
  MockDataStore exec;
  
  public PerMetricQueryPipelineContext(TimeSeriesQuery query,
      QueryContext context, MockDataStore exec, Collection<QueryListener> sinks) {
    super(query, context);
    plan = (net.opentsdb.query.pojo.TimeSeriesQuery) query;
    this.exec = exec;
    this.sinks.addAll(sinks);
  }

  @Override
  public TimeSeriesQuery getQuery(int parallel_id) {
    //return plan.subQueries().get(parallel_id);
    return plan;
  }
  
  @Override
  public TimeSeriesQuery getQuery() {
    return query;
  }
  
  public void initialize() {
    // TODO - pick metric executors
    for (Metric metric : plan.getMetrics()) {
      System.out.println("WORKING METRIC: " + metric);
      // TODO - push down gb
      MDSConfig c = new MDSConfig();
      c.metric = metric;
      c.query = plan;
      if (!Strings.isNullOrEmpty(metric.getFilter())) {
        c.filter = plan.getFilter(metric.getFilter());
      }
      QueryNode node = exec.newNode(this, c);
      //vertices.put(c.id, node);
      graph.addVertex(node);
      
      if (c.filter != null) {
        GBConfig gb_config = null;
        for (TagVFilter v : c.filter.getTags()) {
          if (v.isGroupBy()) {
            if (gb_config == null) {
              gb_config = new GBConfig();
              gb_config.id = "groupBy_" + metric.getId();
            }
            gb_config.tag_keys.add(v.getTagk());
          }
        }
        
        if (gb_config != null) {
          QueryNode gb = new GroupByFactory().newNode(this, gb_config);
          //vertices.put(gb_config.id, gb);
          graph.addVertex(gb);
          try {
            graph.addDagEdge(gb, node);
          } catch (CycleFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          node = gb;
        }
      }
    }
    parseExpresions();
    initializeNodes();
    System.out.println("Built graph: " + graph);
  }
  
  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  void parseExpresions() {
    if (plan.getExpressions() == null || plan.getExpressions().isEmpty()) {
      return;
    }
    
    for (Expression e : plan.getExpressions()) {
      JEConfig config = new JEConfig();
      config.exp = e;
      config.ids_map = Maps.newHashMap();
      
      for (Metric metric : plan.getMetrics()) {
        if (e.getVariables().contains(metric.getId())) {
          config.ids_map.put(metric.getMetric(), metric.getId());
        }
      }
      for (String var : e.getVariables()) {
        if (config.ids_map.containsKey(var)) {
          // must be an expression
          config.ids_map.put(var, var);
        }
      }
      
      QueryNode enode = new JexlExpressionFactory().newNode(this, config);
      graph.addVertex(enode);
      
      final DepthFirstIterator<QueryNode, DefaultEdge> df_iterator = 
        new DepthFirstIterator<QueryNode, DefaultEdge>(graph);
      while (df_iterator.hasNext()) {
        QueryNode node = df_iterator.next();
        if (!graph.outgoingEdgesOf(node).isEmpty()) {
          continue;
        }
        
        if (e.getVariables().contains(node.config().id())) {
          // matched!
          QueryNode upstream = node;
          while (upstream != null) {
            Set<DefaultEdge> incoming = graph.incomingEdgesOf(upstream);
            if (incoming.isEmpty()) {
              break;
            }
            System.out.println("Incoming of " + node.config().id() + " => " + incoming);
            QueryNode us = graph.getEdgeSource(incoming.iterator().next());
            System.out.println("   Upstream was: " + us);
            if (us == null) {
              break;
            }
            upstream = us;
          }
          // walked up, so now add it to the expression
          try {
            System.out.println("LINKING Expression " + e.getId() + " to var: " + node.config().id());
            graph.addDagEdge(enode, upstream);
          } catch (CycleFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }
      }
    }
  }
}

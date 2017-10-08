package net.opentsdb.query;

import java.util.Collection;

import com.google.common.base.Strings;

import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.plan.SplitMetricPlanner;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.processor.GroupByFactory;
import net.opentsdb.query.processor.GroupByFactory.GBConfig;
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

  @Override
  public int parallelQueries() {
    return plan.subQueries().size();
  }
  
  @Override
  public synchronized int nextParallelId() {
    if (parallel_id >= plan.subQueries().size()) {
      parallel_id = 0;
    }
    return parallel_id++;
  }

  public void initialize() {
    // TODO - pick metric executors
    for (Metric metric : plan.getMetrics()) {
      System.out.println("WORKING METRIC: " + metric);
      // TODO - push down gb
      MDSConfig c = new MDSConfig();
      c.id = metric.getId();
      c.metric = metric;
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
          graph.addEdge(gb, node);
          node = gb;
        }
      }
      
      roots.add(node);
      // sink time
//      QueryNode sink = new SinkNode(this, listeners);
//      listeners.add(sink);
//      //vertices.put("sink_" + metric.getId(), sink);
//      graph.addVertex(sink);
//      graph.addEdge(sink, node);
    }
    
    initializeNodes();
    System.out.println("Built graph: " + graph);
  }
  
  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setListener(QueryListener listener) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryListener getListener() {
    // TODO Auto-generated method stub
    return null;
  }
}

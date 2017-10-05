package net.opentsdb.query;

import net.opentsdb.query.plan.SplitMetricPlanner;

public class PerMetricQueryPipelineContext extends AbstractQueryPipelineContext {

  net.opentsdb.query.pojo.TimeSeriesQuery plan;
  int parallel_id = 0;
  
  public PerMetricQueryPipelineContext(TimeSeriesQuery original_query,
      QueryContext context, QueryListener sink) {
    super(original_query, context, sink);
    
    SplitMetricPlanner planner = 
        new SplitMetricPlanner((net.opentsdb.query.pojo.TimeSeriesQuery) original_query);
    plan = planner.getPlannedQuery();
  }

  @Override
  public TimeSeriesQuery getQuery(int parallel_id) {
    return plan.subQueries().get(parallel_id);
  }
  
  @Override
  public TimeSeriesQuery getQuery() {
    return original_query;
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

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
}

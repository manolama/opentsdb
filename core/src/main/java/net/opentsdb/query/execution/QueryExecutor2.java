package net.opentsdb.query.execution;

import java.util.Collection;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryPipeline;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.context.QueryContext2;

public interface QueryExecutor2 {

  public QueryPipeline executeQuery(final QueryPipelineContext context);
  
  public String id();
  
  public Deferred<Object> close();
  
  public Collection<QueryPipeline> outstandingPipelines();
  
}

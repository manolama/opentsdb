package net.opentsdb.query.execution;

import java.util.Collection;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.context.QueryContext2;

public interface QueryExecutor2 {

  public QueryNode executeQuery(final QueryPipelineContext context);
  
  public String id();
  
  public Deferred<Object> close();
  
  public Collection<QueryNode> outstandingPipelines();
  
}

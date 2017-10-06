package net.opentsdb.query.execution;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.graph.ExecutionGraphNode;
import net.opentsdb.utils.Deferreds;

public abstract class AbstractQueryExecutor implements QueryExecutor2 {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractQueryExecutor.class);
  
  protected final ExecutionGraphNode node;
  
  protected final Set<QueryNode> outstanding_pipelines;
  
  protected final List<QueryExecutor2> downstream_executors;
  
  public AbstractQueryExecutor(final ExecutionGraphNode node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (node.getDefaultConfig() == null) {
      throw new IllegalArgumentException("Default config cannot be null.");
    }
    if (node.graph() == null) {
      throw new IllegalStateException("Execution graph cannot be null.");
    }
    this.node = node;
    outstanding_pipelines = Sets.newConcurrentHashSet();
    downstream_executors = Lists.newArrayList();
  }
  
  @Override
  public String id() {
    return node.getExecutorId();
  }
  
  @Override
  public Deferred<Object> close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing executor: " + this);
    }
    cancelOutstanding();
    if (downstream_executors != null) {
      if (downstream_executors.size() == 1) {
        return downstream_executors.iterator().next().close();
      }
      final List<Deferred<Object>> deferreds = 
          Lists.newArrayListWithExpectedSize(downstream_executors.size());
      for (final QueryExecutor2 executor : downstream_executors) {
        deferreds.add(executor.close());
      }
      return Deferred.group(deferreds).addCallback(Deferreds.NULL_GROUP_CB);
    }
    return Deferred.fromResult(null);
  }

  protected void cancelOutstanding() {
    for (final QueryNode pipeline: outstanding_pipelines) {
      try {
        pipeline.close();
      } catch (Exception e) {
        LOG.error("Exception while closing pipeline: " + pipeline, e);
      }
    }
  }

  protected void registerDownstreamExecutor(final QueryExecutor2 executor) {
    downstream_executors.add(executor);
  }
  
  @Override
  public Collection<QueryNode> outstandingPipelines() {
    return outstanding_pipelines;
  }
  
  class CleanupListener implements QueryListener {
    protected QueryListener upstream;
    protected QueryNode pipeline;
    
    @Override
    public void onComplete() {
      outstanding_pipelines.remove(pipeline);
      upstream.onComplete();
    }

    @Override
    public void onNext(final QueryResult next) {
      upstream.onNext(next);
    }

    @Override
    public void onError(final Throwable t) {
      outstanding_pipelines.remove(pipeline);
      upstream.onError(t);
    }
    
  }
  
}

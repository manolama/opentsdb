package net.opentsdb.query.processor.expressions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.Joiner;

/**
 * NOTE that the condition can ONLY be a sub expression and must logical or 
 * relational. 
 */
public class TernaryNode extends BinaryExpressionNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      TernaryNode.class);
  
  protected final Joiner condition_joiner;
  
  public TernaryNode(final QueryNodeFactory factory,
                               final QueryPipelineContext context, 
                               final ExpressionParseNode expression_config) {
    super(factory, context, expression_config);

    final TernaryParseNode config = (TernaryParseNode) expression_config;
    results.put(config.getConditionId(), null);
    condition_joiner = new Joiner(config.getExpressionConfig().getJoin());
  }
  
  @Override
  public void onNext(final QueryResult next) {
    System.out.println("******** DATA: " + next.dataSource());
    if (results.containsKey(next.dataSource())) {
      if (!Strings.isNullOrEmpty(next.error()) || next.exception() != null) {
        sendUpstream(new FailedQueryResult(next));
        return;
      }
      synchronized (this) {
        results.put(next.dataSource(), next);
      }
    } else {
      LOG.warn("Unexpected result at ternary node " + expression_config.getId() 
        + ": " + next.dataSource());
      return;
    }
    
    if (resolveMetrics(next)) {
      // resolving, don't progress yet.
      return;
    }
    
    if (resolveJoinStrings(next)) {
      // resolving, don't progress yet.
      return;
    }
    
    // copy the joins if we have em.
    if (joiner.encodedJoins() != null) {
      condition_joiner.setEncodedJoins(joiner.encodedJoins());
    }
    
    // see if all the results are in.
    int received = 0;
    synchronized (this) {
      for (final QueryResult result : results.values()) {
        if (result != null) {
          received++;
        }
      }
    }
    System.out.println(" RECEIVED: " + received + " out of " + results.size() + "  Results: " + results);
    
    if (received == results.size()) {
      // order is important here.
      final TernaryParseNode config = 
          (TernaryParseNode) expression_config;
      result.join();
      try {
        sendUpstream(result);
      } catch (Exception e) {
        sendUpstream(e);
      }
    }
  }

}

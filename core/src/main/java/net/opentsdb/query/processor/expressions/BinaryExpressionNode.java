package net.opentsdb.query.processor.expressions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.BranchType;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.ExpNodeConfig;

public class BinaryExpressionNode extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryExpressionNode.class);
  
  final ExpressionConfig config;
  final ExpNodeConfig exp_config;
  ExpressionResult result;
  final boolean need_two_sources;
  
  public BinaryExpressionNode(QueryNodeFactory factory,
      QueryPipelineContext context, String id, final ExpressionConfig config, 
      final ExpNodeConfig exp_config) {
    super(factory, context, id);
    this.config = config;
    this.exp_config = exp_config;
    result = new ExpressionResult(this);
    need_two_sources = (exp_config.left_type == BranchType.SUB_EXP || exp_config.left_type == BranchType.VARIABLE) &&
        (exp_config.right_type == BranchType.SUB_EXP || exp_config.right_type == BranchType.VARIABLE);
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
      // TODO - track the source properly
    
  }

  @Override
  public void onNext(QueryResult next) {
    // TODO - track the source properly
    result.add(next);
    if (!need_two_sources || (need_two_sources && result.results.size() == 2)) {
      
      class JoinedCB implements Callback<Object, Object> {
        @Override
        public Object call(Object arg) throws Exception {
          sendUpstream(result);
          completeUpstream(0, 0); // TODO - fix me
          return null;
        }
      }
      result.join().addCallback(new JoinedCB());
    }
  }

  @Override
  public void onError(Throwable t) {
    sendUpstream(t);
  }
}

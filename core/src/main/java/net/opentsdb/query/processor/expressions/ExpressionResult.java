package net.opentsdb.query.processor.expressions;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.Joiner;
import net.opentsdb.query.processor.expressions.ExpressionNodeBuilder.BranchType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.Pair;

// TODO - figure out multi-type joins?

public class ExpressionResult implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionResult.class);

  final BinaryExpressionNode node;
  List<QueryResult> results;
  List<TimeSeries> time_series;
  
  protected ExpressionResult(BinaryExpressionNode node) {
    this.node = node;
    results = Lists.newArrayListWithExpectedSize(2);
  }
  
  void add(QueryResult result) {
    results.add(result);
  }
  
  Deferred<Object> join() {
    if ((node.exp_config.left_type == BranchType.SUB_EXP || node.exp_config.left_type == BranchType.VARIABLE) &&
        (node.exp_config.right_type == BranchType.SUB_EXP || node.exp_config.right_type == BranchType.VARIABLE)) {
      final Joiner joiner = new Joiner(node.exp_config.join);
      Iterable<Pair<TimeSeries, TimeSeries>> joins = joiner.join(results, node.exp_config.left, node.exp_config.right);
      time_series = Lists.newArrayList();
      for (final Pair<TimeSeries, TimeSeries> pair : joins) {
        time_series.add(new ExpressionTimeSeries(node, pair.getKey(), pair.getValue()));
      }
    } else {
      // TODO - one sided join so filter and apply.
    }
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return time_series;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChronoUnit resolution() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RollupConfig rollupConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  
}

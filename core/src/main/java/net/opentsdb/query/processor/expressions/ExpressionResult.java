//This file is part of OpenTSDB.
//Copyright (C) 2018-2020  The OpenTSDB Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package net.opentsdb.query.processor.expressions;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.Pair;

/**
 * The result of a BinaryExpressionNode.
 * 
 * @since 3.0
 */
public class ExpressionResult implements QueryResult {
  /** The parent node. */
  protected final BinaryExpressionNode node;
//  
//  /** The list of 1 or 2 results. 3 if ternary with the condition as 0, 
//   * then left if present and/or right. */
//  protected List<QueryResult> results;
  
  /** Whether or not we're a tenary result. */
  protected final boolean is_ternary;
  
  /** The list of joined time series. */
  protected List<TimeSeries> time_series;
  
  protected QueryResult non_null_result;
  
  /**
   * Package private ctor.
   * @param node A non-null parent node.
   */
  ExpressionResult(final BinaryExpressionNode node) {
    this.node = node;
    is_ternary = node instanceof TernaryNode;
    //results = Lists.newArrayListWithExpectedSize(is_ternary ? 3 : 2);
  }
  
//  /**
//   * Package private method to add a result.
//   * @param result A non-null result.
//   * Note that we don't check the Id types here.
//   */
//  void add(final QueryResult result) {
//    if (results == null) {
//      results = Lists.newArrayList(result);
//    } else {
//      results.add(result);
//    }
//  }
  
  /**
   * Package private method called by the node when it has seen all the
   * results it needs for the expression.
   */
  void join() {
    final Iterable<TimeSeries[]> joins;
    final ExpressionParseNode config = (ExpressionParseNode) node.config();
    
    if ((config.getLeftType() == OperandType.SUB_EXP || 
        config.getLeftType() == OperandType.VARIABLE) &&
        (config.getRightType() == OperandType.SUB_EXP || 
        config.getRightType() == OperandType.VARIABLE)) {
      final boolean use_alias = 
          config.getLeftType() != OperandType.VARIABLE ||
              config.getRightType() != OperandType.VARIABLE;
      System.out.println("           LEFT M: " + node.leftMetric() + "  R: " + node.rightMetric());
      joins = node.joiner().join(node.results().values(), 
          node.leftMetric() != null ? node.leftMetric() : 
            ((String) config.getLeft()).getBytes(Const.UTF8_CHARSET), 
          node.rightMetric() != null ? node.rightMetric() : 
            ((String) config.getRight()).getBytes(Const.UTF8_CHARSET),
          use_alias,
          is_ternary);
    } else if (config.getLeftType() == OperandType.SUB_EXP || 
        config.getLeftType() == OperandType.VARIABLE) {
      final boolean use_alias = 
          config.getLeftType() != OperandType.VARIABLE;
      // left
      joins = node.joiner().join(
          node.results().values(), 
          node.leftMetric() != null ? node.leftMetric() : 
            ((String) config.getLeft()).getBytes(Const.UTF8_CHARSET), 
          true,
          use_alias);
    } else {
      final boolean use_alias = 
          config.getRightType() != OperandType.VARIABLE;
      // right
      joins = node.joiner().join(
          node.results().values(), 
          node.rightMetric() != null ? node.rightMetric() :
            ((String) config.getRight()).getBytes(Const.UTF8_CHARSET), 
          false, 
          use_alias);
    }
    
    time_series = Lists.newArrayList();
    for (final TimeSeries[] pair : joins) {
      // TODO TERNARY
      time_series.add(new ExpressionTimeSeries(node, this, pair[0], pair[1]));
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return time_series;
  }

  @Override
  public String error() {
    // TODO - implement
    return null;
  }
  
  @Override
  public Throwable exception() {
    // TODO - implement
    return null;
  }
  
  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return node.config().resultIds().get(0);
  }
  
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.idType();
  }

  @Override
  public ChronoUnit resolution() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    if (non_null_result == null) {
      non_null_result = node.results().values().iterator().next();
    }
    return non_null_result.rollupConfig();
  }

  @Override
  public void close() {
    for (final Entry<QueryResultId, QueryResult> entry : node.results().entrySet()) {
      entry.getValue().close();
    }
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

}
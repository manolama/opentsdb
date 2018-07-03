package net.opentsdb.query.processor.expressions;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

public class ExpressionTimeSeries implements TimeSeries {

  final BinaryExpressionNode node;
  final TimeSeries left;
  final TimeSeries right;
  final TimeSeriesId id;
  
  public ExpressionTimeSeries(BinaryExpressionNode node,
      TimeSeries left, TimeSeries right) {
    this.node = node;
    this.left = left;
    this.right = right;
    // TODO - id from joiner.
    id = node.joiner.joinIds(left, right, node.id());
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      TypeToken<?> type) {
    // TODO Auto-generated method stub
    if (type == NumericType.TYPE) {
      System.out.println(" RET NUMERIC");
      return Optional.of(new ExpressionNumericTypeIterator(this));
    }
    return Optional.empty();
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    // TODO Auto-generated method stub
    List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = Lists.newArrayListWithExpectedSize(1);
    iterators.add(new ExpressionNumericTypeIterator(this));
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    // TODO - more types
    return Lists.newArrayList(NumericType.TYPE);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

}

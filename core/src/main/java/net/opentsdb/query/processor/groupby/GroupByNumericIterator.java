package net.opentsdb.query.processor.groupby;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregator.Doubles;
import net.opentsdb.core.Aggregator.Longs;
import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.TimeStampComparator;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;
import net.opentsdb.utils.Deferreds;

public class GroupByNumericIterator extends TimeSeriesIterator<NumericType> {

  /** Reference to the processor config. */
  private final GroupByConfig config;
  
  private final Aggregator aggregator;
  
  /** Map of variable name to iterators to run through the context. */ 
  private final List<TimeSeriesIterator<NumericType>> iterators;
  
  /** The ID merger. */
  private final MergedTimeSeriesId.Builder merger;
  
  private TimeSeriesValue<NumericType>[] values;
  
  private TimeStamp ts;
  
  /** The data point we'll update and send upstream. */
  private MutableNumericType dp;
  
  public GroupByNumericIterator(final QueryContext context,
                                final GroupByConfig config,
                                final Aggregator aggregator) {
    super();
    setContext(context);
    this.config = config;
    this.aggregator = aggregator;
    iterators = Lists.newArrayList();
    // TODO - need sync info from the other branch once it's up.
    ts = new MillisecondTimeStamp(0L);
    ts.setMax();
    merger = MergedTimeSeriesId.newBuilder();
  }

  public void addIterator(final TimeSeriesIterator<NumericType> it) {
    if (it == null) {
      throw new IllegalArgumentException("Iterator source cannot be null.");
    }
    iterators.add(it);
    if (context != null) {
      context.register(this, it);
    }
    merger.addSeries(it.id());
  }
  
  @Override
  public Deferred<Object> initialize() {
    values = new TimeSeriesValue[iterators.size()];
    for (int i = 0; i < iterators.size(); i++) {
      final TimeSeriesIterator<NumericType> it = iterators.get(i);
      if (it.status() != IteratorStatus.HAS_DATA) {
        continue;
      }
      values[i] = it.next();
      System.out.println("INIT V: " + values[i]);
      if (values[i].timestamp().compare(TimeStampComparator.LT, ts)) {
        ts.update(values[i].timestamp());
      }
    }
    System.out.println("INIT TS: " + ts);
    dp = new MutableNumericType(merger.build());
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSeriesId id() {
    return dp.id();
  }
  
  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericType.TYPE;
  }

  @Override
  public TimeStamp startTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeStamp endTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IteratorStatus status() {
    if (context != null) {
      throw new IllegalStateException("Iterator is a part of a context.");
    }
    IteratorStatus status = IteratorStatus.END_OF_DATA;
    for (final TimeSeriesIterator<?> it : iterators) {
      status = IteratorStatus.updateStatus(status, it.status());
    }
    return status;
  }

  @Override
  public TimeSeriesValue<NumericType> next() {
    int reals = 0;
    DoubleAccumulator doubles = null;
    //LongAccumulator longs = null;
    
    final TimeStamp next = new MillisecondTimeStamp(0L);
    next.setMax();
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        continue;
      }
      
      if (values[i].timestamp().compare(TimeStampComparator.NE, ts)) {
        if (values[i].timestamp().compare(TimeStampComparator.LT, next)) {
          next.update(values[i].timestamp());
        }
        continue;
      }
      
//      if (longs == null && doubles == null) {
//       if (values[i].value().isInteger()) {   
//          longs = new LongAccumulator();
//        } else if (i == 0) {
//          doubles = new DoubleAccumulator();
//        }
//      }
//      
//      if (!values[i].value().isInteger() && longs != null) {
//        doubles = new DoubleAccumulator();
//        doubles.move(longs);
//        longs = null;
//      }
//      
//      if (longs != null) {
//        longs.longs.add(values[i].value().longValue());
//      } else {
        doubles.doubles.add(values[i].value().toDouble());
      //}
      reals += values[i].realCount();
      
      if (iterators.get(i).status() == IteratorStatus.HAS_DATA) {
        values[i] = iterators.get(i).next();
        if (values[i].timestamp().compare(TimeStampComparator.LT, next)) {
          next.update(values[i].timestamp());
        }
      }
    }
    
    //if (doubles != null) {
      dp.reset(ts, aggregator.runDouble(doubles), reals);
    //} else if (longs != null) {
    //  dp.reset(ts, aggregator.runLong(longs), reals);
    //} else {
    //  throw new IllegalStateException("WTF??? Nulls for both?");
    //}
    
    ts.update(next);
    return dp;
  }

  @Override
  public TimeSeriesValue<NumericType> peek() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeSeriesIterator<NumericType> getShallowCopy(QueryContext context) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeSeriesIterator<NumericType> getDeepCopy(QueryContext context,
      TimeStamp start, TimeStamp end) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected void updateContext() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Deferred<Object> fetchNext() {
    final List<Deferred<Object>> deferreds = 
        Lists.newArrayListWithExpectedSize(iterators.size());
    for (final TimeSeriesIterator<?> it : iterators) {
      deferreds.add(it.fetchNext());
    }
    return Deferred.group(deferreds).addBoth(Deferreds.NULL_GROUP_CB);
  }
  
  static class DoubleAccumulator implements Doubles {

    int idx = 0;
    List<Double> doubles = Lists.newArrayList();
    
    @Override
    public boolean hasNextValue() {
      return idx < doubles.size();
    }

    @Override
    public double nextDoubleValue() {
      return doubles.get(idx++);
    }
    
    public void move(final LongAccumulator longs) {
      for (final long v : longs.longs) {
        doubles.add((double) v);
      }
    }
  }
  
  static class LongAccumulator implements Longs {

    int idx = 0;
    List<Long> longs = Lists.newArrayList();
    
    @Override
    public boolean hasNextValue() {
      return idx < longs.size();
    }

    @Override
    public long nextLongValue() {
      return longs.get(idx++);
    }
    
  }
}

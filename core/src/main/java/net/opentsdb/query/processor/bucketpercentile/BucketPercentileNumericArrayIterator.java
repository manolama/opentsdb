package net.opentsdb.query.processor.bucketpercentile;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryIterator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

public class BucketPercentileNumericArrayIterator implements TimeSeries, 
    QueryIterator, 
    TimeSeriesValue<NumericArrayType>, 
    NumericArrayType{
  
  private final TimeStamp timestamp;
  private final double[] percentile;
  private boolean has_next;
  private TimeSeriesId id;
  private final TimeSeriesId base_id;
  private final String as;
  private final double ptile;
    
  public BucketPercentileNumericArrayIterator(final TimeStamp timestamp,
                                              final double[] percentile,
                                              final TimeSeriesId base_id,
                                              final String as,
                                              final double ptile) {
    this.timestamp = timestamp;
    this.percentile = percentile;
    this.base_id = base_id;
    this.as = as;
    this.ptile = ptile;
    has_next = true;
  }
  
  @Override
  public TypeToken getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return has_next;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    has_next = false;
    return this;
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return percentile.length;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public long[] longArray() {
    return null;
  }

  @Override
  public double[] doubleArray() {
    return percentile;
  }

  @Override
  public TimeStamp timestamp() {
    return timestamp;
  }

  @Override
  public NumericArrayType value() {
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeSeriesId id() {
    if (id != null) {
      return id;
    }
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(as)
        .setTags(Maps.newHashMap(((TimeSeriesStringId) base_id).tags()))
        //.setAggregatedTags(((TimeSeriesStringId) base_id).aggregatedTags())
        //.setDisjointTags(((TimeSeriesStringId) base_id).disjointTags())
        .addTags("_percentile", Double.toString(ptile * 100))
        .build();
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericArrayType.TYPE) {
      return Optional.empty();
    }
    return Optional.of((TypedTimeSeriesIterator<? extends TimeSeriesDataType>) this);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = this;
    return Lists.<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>newArrayList(it);
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return NumericArrayType.SINGLE_LIST;
  }

}

package net.opentsdb.query.processor.bucketquantile;

import java.util.Collection;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.NumericSummaryType;

public class BucketQuantileNumericSummaryIterator implements TimeSeries, 
    TypedTimeSeriesIterator<NumericSummaryType> {

  private final int quantile_index;
  private final BucketQuantileNumericSummaryComputation processor;
  private final MutableNumericSummaryValue dp;
  private TimeSeriesId id;
  private int index;
  
  BucketQuantileNumericSummaryIterator(
      final int quantile_index,
      final BucketQuantileNumericSummaryComputation processor) {
    this.quantile_index = quantile_index;
    this.processor = processor;
    dp = new MutableNumericSummaryValue();
  }

  @Override
  public TypeToken getType() {
    return NumericSummaryType.TYPE;
  }

  @Override
  public boolean hasNext() {
    return index < processor.limit;
  }

  @Override
  public TimeSeriesValue<NumericSummaryType> next() {
    dp.timestamp().updateEpoch(processor.timestamps[index]);
    dp.resetValue(processor.summary, processor.quantiles[quantile_index][index]);
    index++;
    return dp;
  }
  
  @Override
  public TimeSeriesId id() {
    if (id != null) {
      return id;
    }
    // TODO - byte id
    id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(((BucketQuantileConfig) processor.node.config()).getAs())
        .setTags(Maps.newHashMap(((TimeSeriesStringId) processor.id).tags()))
        // TODO - ?
        //.setAggregatedTags(((TimeSeriesStringId) base_id).aggregatedTags())
        //.setDisjointTags(((TimeSeriesStringId) base_id).disjointTags())
        .addTags(BucketQuantileFactory.PERCENTILE_TAG, Double.toString(
            ((BucketQuantileConfig) processor.node.config()).getQuantiles().get(quantile_index) * 100))
        .build();
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type != NumericSummaryType.TYPE) {
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
    return NumericSummaryType.SINGLE_LIST;
  }

  @Override
  public void close() {
    // no-op
  }
  
}

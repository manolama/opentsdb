package net.opentsdb.query.egads;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryResult;

public class EgadsTimeSeries implements TimeSeries {
  public static final String MODEL_TAG_KEY = "_anomalyModel";
  
  protected final TimeSeries source;
  protected final TimeSeriesId id;
  
  public EgadsTimeSeries(final TimeSeries source, 
                         final String suffix, 
                         final String model) {
    this.source = source;
    // TODO - handle byte IDs somehow.
    final TimeSeriesStringId string_id = (TimeSeriesStringId) source.id();
    BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder()
        .setAlias(string_id.alias())
        .setNamespace(string_id.namespace())
        .setMetric(string_id.metric() + "." + suffix)
        .setAggregatedTags(string_id.aggregatedTags())
        .setDisjointTags(string_id.disjointTags());
    final Map<String, String> tags = Maps.newHashMap();
    if (string_id.tags() != null) {
      tags.putAll(string_id.tags());
    }
    tags.put(MODEL_TAG_KEY, model);
    builder.setTags(tags);
    this.id = builder.build();
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    return source.iterator(type);
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    return source.iterators();
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    return source.types();
  }

  @Override
  public void close() {
    // no-op
  }
  
}

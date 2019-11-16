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
  
  protected final TimeSeriesId id;
  protected final TimeStamp timestamp;
  protected final long original_hash;
  protected final double[] results;
  protected List<AlertValue> alerts;
  
  public EgadsTimeSeries(final TimeSeriesId id, 
                  final double[] results, 
                  final String suffix, 
                  final String model,
                  final TimeStamp timestamp) {
    original_hash = id.buildHashCode();
    this.timestamp = timestamp;
    // TODO - handle byte IDs somehow.
    final TimeSeriesStringId string_id = (TimeSeriesStringId) id;
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
    this.results = results;
  }
  
  public void addAlerts(final List<AlertValue> results) {
    alerts = results;
  }
  
  public void addAlert(final AlertValue alert) {
    if (alerts == null) {
      alerts = Lists.newArrayList();
    }
    alerts.add(alert);
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
      TypeToken<? extends TimeSeriesDataType> type) {
    if (type == NumericArrayType.TYPE) {
      return Optional.of(new ArrayIterator());
    } else if (type == AlertType.TYPE && alerts != null && !alerts.isEmpty()) {
      return Optional.of(new AlertIterator());
    }
    return Optional.empty();
  }

  @Override
  public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
    final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
        Lists.newArrayList();
    its.add(new ArrayIterator());
    if (alerts != null && !alerts.isEmpty()) {
      its.add(new AlertIterator());
    }
    return its;
  }

  @Override
  public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
    if (alerts == null || alerts.isEmpty()) {
      if (results == null || results.length < 1) {
        return Collections.emptySet();
      }
      return Sets.newHashSet(NumericArrayType.TYPE);
    } else {
      return Sets.newHashSet(NumericArrayType.TYPE, AlertType.TYPE);
    }
  }

  @Override
  public void close() {
    // no-op
  }
  
  /** @return The original hash of the original ID before we add the suffix. */
  public long originalHash() {
    return original_hash;
  }
  
  class AlertIterator implements TypedTimeSeriesIterator<AlertType> {
    int idx = 0;
    
    @Override
    public boolean hasNext() {
      return idx < alerts.size();
    }

    @Override
    public TimeSeriesValue<AlertType> next() {
      return alerts.get(idx++);
    }

    @Override
    public TypeToken<AlertType> getType() {
      return AlertType.TYPE;
    }
    
  }
  
  class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, NumericArrayType {

    boolean flipflop = (results != null && results.length > 0) ? true : false;
    
    @Override
    public boolean hasNext() {
      return flipflop;
    }

    @Override
    public TimeSeriesValue<NumericArrayType> next() {
      flipflop = false;
      return this;
    }

    @Override
    public int offset() {
      return 0;
    }

    @Override
    public int end() {
      return results.length;
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
      return results;
    }

    @Override
    public TypeToken<NumericArrayType> getType() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TypeToken<NumericArrayType> type() {
      return NumericArrayType.TYPE;
    }

    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public NumericArrayType value() {
      return this;
    }
    
  }
  
}

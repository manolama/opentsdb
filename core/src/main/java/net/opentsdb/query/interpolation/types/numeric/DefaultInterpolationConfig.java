package net.opentsdb.query.interpolation.types.numeric;

import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.QueryInterpolationConfig;
import net.opentsdb.query.QueryInterpolatorConfig;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.QueryIteratorInterpolatorFactory;

public class DefaultInterpolationConfig implements QueryInterpolationConfig {

  private Map<TypeToken<?>, Container> configs;
  
  DefaultInterpolationConfig(final Builder builder) {
    configs = builder.configs;
  }
  
  @Override
  public QueryInterpolatorConfig config(
      TypeToken<? extends TimeSeriesDataType> type) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.config;
  }

  @Override
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      TypeToken<? extends TimeSeriesDataType> type, TimeSeries source, QueryInterpolatorConfig config) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, source, config);
  }

  @Override
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      TypeToken<? extends TimeSeriesDataType> type, TimeSeries source) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, source, container.config);
  }

  @Override
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      TypeToken<? extends TimeSeriesDataType> type,
      Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator,
      QueryInterpolatorConfig config) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, iterator, config);
  }

  @Override
  public QueryIteratorInterpolator<? extends TimeSeriesDataType> newInterpolator(
      TypeToken<? extends TimeSeriesDataType> type,
      Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> iterator) {
    final Container container = configs.get(type);
    if (container == null) {
      return null;
    }
    return container.factory.newInterpolator(type, iterator, container.config);
  }

  static class Container {
    public QueryInterpolatorConfig config;
    public QueryIteratorInterpolatorFactory factory;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private Map<TypeToken<?>, Container> configs = Maps.newHashMap();
    
    public Builder add(final TypeToken<?> type, QueryInterpolatorConfig config, QueryIteratorInterpolatorFactory factory) {
      Container container = configs.get(type);
      if (container == null) {
        container = new Container();
        configs.put(type, container);
      }
      container.config = config;
      container.factory = factory;
      return this;
    }
    
    public DefaultInterpolationConfig build() {
      return new DefaultInterpolationConfig(this);
    }
  }
  
}

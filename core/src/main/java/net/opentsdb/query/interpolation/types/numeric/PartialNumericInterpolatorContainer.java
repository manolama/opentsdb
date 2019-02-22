package net.opentsdb.query.interpolation.types.numeric;

import java.util.Comparator;
import java.util.TreeMap;

import com.google.common.collect.Maps;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.pools.MutableNumericValuePool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.Poolable;
import net.opentsdb.query.interpolation.PartialQueryInterpolator;
import net.opentsdb.query.interpolation.PartialQueryInterpolatorContainer;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.utils.Pair;

public class PartialNumericInterpolatorContainer implements 
    PartialQueryInterpolatorContainer<NumericType> {
  
  protected final TSDB tsdb;
  
  /** The config. */
  protected NumericInterpolatorConfig config;
  
  protected final TreeMap<PartialTimeSeries, Pair<MutableNumericValue, MutableNumericValue>> map;
  
  protected Poolable poolable;
  
  public PartialNumericInterpolatorContainer(final TSDB tsdb) {
    this.tsdb = tsdb;
    map = Maps.newTreeMap(COMPARATOR);
  }
  
  @Override
  public void close() throws Exception {
    if (poolable != null) {
      poolable.release();
    }
  }

  @Override
  public void setConfig(QueryInterpolatorConfig config) {
    this.config = (NumericInterpolatorConfig) config;
    for (final Pair<MutableNumericValue, MutableNumericValue> pair : map.values()) {
      try {
        pair.getKey().close();
      } catch (Exception e) {
        // piffle.
      } 
    }
    // TODO - reset others
  }

  // UGGG but we want the first and last of other partials before we work with
  // another!
  // To top it off, order could be indeterministic.
  @Override
  public synchronized PartialQueryInterpolator<NumericType> newPartial(PartialTimeSeries series) {
    final TypedTimeSeriesIterator it = series.iterator();
    Pair<MutableNumericValue, MutableNumericValue> pair = new Pair<>(null, null);
    
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> value = (TimeSeriesValue<NumericType>) it.next();
      if (value.value() == null) {
        continue;
      }
      
      final ObjectPool mutables = 
          tsdb.getRegistry().getObjectPool(MutableNumericValuePool.TYPE);
      if (pair.getKey() == null) {
        MutableNumericValue new_value = (MutableNumericValue) mutables.claim().object();
        new_value.reset(value);
        pair.setKey(new_value);
      }
      
      if (!it.hasNext()) {
        // last one.
        MutableNumericValue new_value = (MutableNumericValue) mutables.claim().object();
        new_value.reset(value);
        pair.setValue(new_value);
      }
    }
    
    map.put(series, pair);
    
    final ObjectPool interpolators = 
        tsdb.getRegistry().getObjectPool(PartialNumericInterpolatorPool.TYPE);
    PartialNumericInterpolator interpolator = (PartialNumericInterpolator) interpolators.claim().object();
    interpolator.reset(this, series);
    return interpolator;
  }
  
  static class TimeComparator implements Comparator<PartialTimeSeries> {

    @Override
    public int compare(PartialTimeSeries o1, PartialTimeSeries o2) {
      return TimeStamp.COMPARATOR.compare(o1.shard().start(), o2.shard().start());
    }
    
  }
  static final TimeComparator COMPARATOR = new TimeComparator();

  @Override
  public void setPoolable(Poolable poolable) {
    this.poolable = poolable;
  }

  @Override
  public QueryInterpolatorConfig config() {
    // TODO Auto-generated method stub
    return config;
  }
}

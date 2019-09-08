package net.opentsdb.query.execution.cache;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
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
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.cache.QueryCachePlugin.CachedQueryResult;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdes;
import net.opentsdb.query.serdes.TimeSeriesCacheSerdesFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes.ByteArrayKey;

public class DummyCacheSerdes extends BaseTSDBPlugin implements TimeSeriesCacheSerdesFactory, TimeSeriesCacheSerdes {

  @Override
  public byte[] serialize(Collection<QueryResult> results) {
    // TODO Auto-generated method stub
    return new byte[] { 42 };
  }
  
  @Override
  public Map<String, CachedQueryResult> deserialize(final byte[] data) {
    
    class RS implements CachedQueryResult {
      MockTimeSeries mts;
      RS() {
        mts = new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
            .setMetric("sys.if.in")
            .addTags("host", "web02")
            .addTags("dc", "LGA")
            .build());
        MutableNumericValue v = new MutableNumericValue(new SecondTimeStamp(1564086600), 505055154);
        mts.addValue(v);
        v = new MutableNumericValue(new SecondTimeStamp(1564086660), 68134887);
        mts.addValue(v);
      }
      
      @Override
      public TimeSpecification timeSpecification() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Collection<TimeSeries> timeSeries() {
        System.out.println(" ***************** WOOT ");
        return Lists.newArrayList(mts);
      }

      @Override
      public String error() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Throwable exception() {
        // TODO Auto-generated method stub
        return null;
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
      public String dataSource() {
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

      @Override
      public TimeStamp lastValueTimestamp() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
    
    Map<String, CachedQueryResult> map = Maps.newHashMap();
    map.put("ds:m1", new RS());
    return map;
  }

  @Override
  public TimeSeriesCacheSerdes getSerdes() {
    return this;
  }

  @Override
  public String type() {
    return "DummyCacheSerdes";
  }

  public class MockTimeSeries implements TimeSeries {

    /** The non-null ID. */
    protected final TimeSeriesStringId id;
    
    /** Whether or not we should sort when returning iterators. */
    protected final boolean sort;
    
    /** Whether or not closed has been called. */
    protected boolean closed;
    
    /** The map of types to lists of time series. */
    protected Map<TypeToken<? extends TimeSeriesDataType>, 
      List<TimeSeriesValue<?>>> data;
    
    /**
     * Default ctor.
     * @param id A non-null Id.
     */
    public MockTimeSeries(final TimeSeriesStringId id) {
      this(id, false);
    }
    
    /**
     * Alternate ctor to set sorting.
     * @param id A non-null Id.
     * @param sort Whether or not to sort on timestamps on the output.
     */
    public MockTimeSeries(final TimeSeriesStringId id, final boolean sort) {
      this.sort = sort;
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      this.id = id;
      data = Maps.newHashMap();
    }
    
    /**
     * @param value A non-null value to add to the proper array. Must return a type.
     */
    public void addValue(final TimeSeriesValue<?> value) {
      if (value == null) {
        throw new IllegalArgumentException("Can't store null values.");
      }
      List<TimeSeriesValue<?>> types = data.get(value.type());
      if (types == null) {
        types = Lists.newArrayList();
        data.put(value.type(), types);
      }
      types.add(value);
    }
    
    /** Flushes the map of data but leaves the ID alone. Also resets 
     * the closed flag. */
    public void clear() {
      data.clear();
      closed = false;
    }
    
    @Override
    public TimeSeriesStringId id() {
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      List<TimeSeriesValue<? extends TimeSeriesDataType>> types = data.get(type);
      if (types == null) {
        return Optional.empty();
      }
      if (sort) {
        Collections.sort(types, new TimeSeriesValue.TimeSeriesValueComparator());
      }
      TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = new MockTimeSeriesIterator(types.iterator(), type);
      return Optional.of(it);
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators
        = Lists.newArrayListWithCapacity(data.size());
      for (final Entry<TypeToken<? extends TimeSeriesDataType>, 
          List<TimeSeriesValue<?>>> entry : data.entrySet()) {
        
        iterators.add(new MockTimeSeriesIterator(entry.getValue().iterator(), entry.getKey()));
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return data.keySet();
    }

    @Override
    public void close() {
      closed = true;
    }

    public boolean closed() {
      return closed;
    }
    
    public Map<TypeToken<? extends TimeSeriesDataType>, 
        List<TimeSeriesValue<?>>> data() {
      return data;
    }
    
    /**
     * Iterator over the list of values.
     */
    class MockTimeSeriesIterator implements TypedTimeSeriesIterator {
      private final Iterator<TimeSeriesValue<?>> iterator;
      private final TypeToken<? extends TimeSeriesDataType> type;
      
      MockTimeSeriesIterator(final Iterator<TimeSeriesValue<?>> iterator,
                             final TypeToken<? extends TimeSeriesDataType> type) {
        this.iterator = iterator;
        this.type = type;
      }
      
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public TimeSeriesValue<?> next() {
        return iterator.next();
      }
      
      @Override
      public TypeToken<? extends TimeSeriesDataType> getType() {
        return type;
      }
      
    }
    
  }

  @Override
  public byte[][] serialize(int[] timestamps, 
      Collection<QueryResult> results) {
    // TODO Auto-generated method stub
    return null;
  }

}

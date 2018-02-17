package net.opentsdb.storage.schemas.v1;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.openjdk.jol.info.ClassLayout;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.storage.StorageSeries;
import net.opentsdb.utils.ConcurrentByteMap;

public class V1Result implements QueryResult, Runnable {

  // TODO - double check that instance size includes header and pointer so we don't need
  // .headerSize().
  public static long MAP_SIZE = ClassLayout.parseClass(ConcurrentHashMap.class).instanceSize();
  public static long TYPE_TOKEN_SIZE = ClassLayout.parseClass(TypeToken.class).instanceSize();
  public static long STORAGE_SERIES_SIZE = ClassLayout.parseClass(StorageSeries.class).instanceSize();
  public static long BYTE_ARRAY_SIZE = ClassLayout.parseClass(byte[].class).instanceSize();
  
  private ConcurrentByteMap<ConcurrentHashMap<TypeToken<?>, StorageSeries>> iterators = 
      new ConcurrentByteMap<ConcurrentHashMap<TypeToken<?>, StorageSeries>>();
  
  private V1Schema schema;
  private List<TimeSeries> timeseries;
  
  private final CountDownLatch countdown;
  private final QueryNode node;

  private int sequence_id;
  private Exception ex;
  
  // TODO - set these for real
  private long size_limit = 1024;
  private AtomicLong size = new AtomicLong();
  
  public V1Result(final V1SourceNode node) {
    countdown = new CountDownLatch(node.parallelProcesses());
    this.node = node;
  }
  
  @Override
  public void run() {
    timeseries = Lists.newArrayListWithCapacity(iterators.size());
    final Iterator<Entry<byte[], ConcurrentHashMap<TypeToken<?>, StorageSeries>>> it = iterators.entrySet().iterator();
    while (it.hasNext()) {
      final Entry<byte[], ConcurrentHashMap<TypeToken<?>, StorageSeries>> entry = it.next();
      it.remove(); // free it up for GC.
      timeseries.add(new LocalTS(entry.getKey(), entry.getValue()));
    }
    iterators = null; // let it go so GC can do it's thang!
  }

  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return timeseries;
  }

  @Override
  public long sequenceId() {
    return sequence_id;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public void close() {
    // TODO - correct?
    node.close();
  }

  /**
   * TODO - did it this way instead of taking in a List<KeyValue> row because we want to
   * be able to re-use this schema for other stores like BigTable.
   * @param base
   * @param tsuid
   * @param prefix
   * @param qualifier
   * @param value
   */
  public void addData(final TimeStamp base, final byte[] tsuid, final byte prefix, final byte[] qualifier, final byte[] value) {
    // TODO - if we have too much data, return false so the scanner can cache it 
    // for the next call.
    
    // decode
    final V1Codec codec = schema.getCodec(prefix);
    if (codec == null) {
      throw new RuntimeException("WTF?");
    }
    TypeToken<?> type = schema.prefixToType(prefix);
    
    // TODO - better size calc
    size.addAndGet(qualifier.length + value.length);
    
    ConcurrentHashMap<TypeToken<?>, StorageSeries> type_map = iterators.get(tsuid);
    if (type_map == null) {
      type_map = new ConcurrentHashMap<TypeToken<?>, StorageSeries>();
      ConcurrentHashMap<TypeToken<?>, StorageSeries> extant = iterators.putIfAbsent(tsuid, type_map);
      if (extant != null) {
        type_map = extant;
      } else {
        size.addAndGet(MAP_SIZE + tsuid.length);
      }
    }
    
    StorageSeries iterator = type_map.get(type);
    if (iterator == null) {
      iterator = codec.newIterable();
      StorageSeries extant = type_map.putIfAbsent(type, iterator);
      if (extant != null) {
        iterator = extant;
      } else {
        size.addAndGet(TYPE_TOKEN_SIZE + STORAGE_SERIES_SIZE);
      }
    }
    iterator.decode(base, tsuid, prefix, qualifier, value);
  }
  
  public boolean isFull() {
    if (size_limit < 1) {
      return false;
    }
    return size.get() >= size_limit;
  }
  
  public synchronized boolean hasException() {
    return ex != null;
  }
  
  public synchronized void setException(final Exception ex) {
    if (ex == null) {
      this.ex = ex;
      node.onError(ex);
    }
  }
  
  public void scannerDone() {
    // called to mark the scanner as done with it's current workload.
    countdown.countDown();
    if (countdown.getCount() == 0) {
      run(); // TODO - may need to move to another pool!
      node.onNext(this);
    }
  }
  
  public void setSequenceId(final int sequence_id) {
    this.sequence_id = sequence_id;
  }
  
  class LocalTS implements TimeSeries {
    private final TimeSeriesId id;
    private final ConcurrentHashMap<TypeToken<?>, StorageSeries> series;
    
    LocalTS(final byte[] tsuid, final ConcurrentHashMap<TypeToken<?>, StorageSeries> series) {
      id = new V1TimeSeriesId(tsuid);
      this.series = series;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      final StorageSeries ts = series.get(type);
      if (ts == null) {
        return Optional.empty();
      }
      return Optional.of(ts.iterator());
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      final List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators
         = Lists.newArrayListWithCapacity(series.size());
      for (final StorageSeries ts : series.values()) {
        iterators.add(ts.iterator());
      }
      return iterators;
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return series.keySet();
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
  }
}

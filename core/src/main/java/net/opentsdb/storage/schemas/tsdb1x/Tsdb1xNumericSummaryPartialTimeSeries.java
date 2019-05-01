package net.opentsdb.storage.schemas.tsdb1x;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericByteArraySummaryType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.IllegalDataException;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xNumericPartialTimeSeries.ReAllocatedArray;
import net.opentsdb.utils.Bytes;

public class Tsdb1xNumericSummaryPartialTimeSeries implements Tsdb1xPartialTimeSeries<NumericByteArraySummaryType>, NumericByteArraySummaryType {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xNumericSummaryPartialTimeSeries.class);
  
  /** Reference to the Object pool for this instance. */
  protected PooledObject pooled_object;
  
  /** A hash for the time series ID. */
  protected long id_hash;
  
  /** The set we currently belong to. */
  protected PartialTimeSeriesSet set;
  
  /** A reference counter for the array to determine when we can return it to
   * the pool. */
  protected AtomicInteger reference_counter;
  
  /** An array to store the data in. */
  protected PooledObject pooled_array;
  
  /** The current write index. */
  protected int write_idx;
  
  /** Whether or not the array has out-of-order or duplicate data. */
  protected boolean needs_repair;
  
  /** The last offset, used to determine if we need a repair. */
  protected int last_offset = -1;
  
  protected byte last_type = -1;
  
  protected TimeStamp base_timestamp;
  
  protected ObjectPool byte_array_pool;
  
  protected RollupInterval interval;
  
  public Tsdb1xNumericSummaryPartialTimeSeries() {
    reference_counter = new AtomicInteger();
    base_timestamp = new SecondTimeStamp(0);
  }
  
  public void reset(final TimeStamp base_timestamp, 
                    final long id_hash, 
                    final ObjectPool byte_array_pool,
                    final PartialTimeSeriesSet set,
                    final RollupInterval interval) {
    if (set == null) {
      throw new RuntimeException("NULL SET FROM below...");
    }

    this.id_hash = id_hash;
    this.set = set;
    this.base_timestamp.update(base_timestamp);
    this.byte_array_pool = byte_array_pool;
    this.interval = interval;
  }

  @Override
  public void close() throws Exception {
    release();
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (reference_counter.decrementAndGet() == 0) {
      if (pooled_array != null) {
        pooled_array.release();
        pooled_array = null;
      }
      if (pooled_object != null) {
        pooled_object.release();
      }
      set = null;
      write_idx = 0;
      needs_repair = false;
      last_offset = -1;
      last_type = -1;
      System.out.println("    RELEASE: " + System.identityHashCode(this));
      //new RuntimeException().printStackTrace();
    }
  }
  
  @Override
  public void setEmpty(final PartialTimeSeriesSet set) {
    this.set = set;
    if (set == null) {
      throw new RuntimeException("NULL SET FROM below...");
    }
  }

  @Override
  public void addColumn(final byte prefix,
                        final byte[] qualifier, 
                        final byte[] value) {
    if (qualifier.length < 2) {
      throw new IllegalDataException("Qualifier was too short.");
    }
    if (value.length < 1) {
      throw new IllegalDataException("Value was too short.");
    }
    if (pooled_array == null) {
      pooled_array = byte_array_pool.claim();
      if (pooled_array == null) {
        throw new IllegalStateException("The pooled array was null!");
      }
    }
    
    // so, we can have byte prefix's or we could have strings for the old
    // rollups. E.g. [ type, offset, offset ] or 
    // [ 'a', 'g', 'g', ':', offset, offset ]. Offsets may be on 2 or 
    // more bytes. So we can *guess* that if the length is 5 or less 
    // that it's a byte prefix, and > 5 then it's a string prefix.
    // ALSO note that appends are <offset><value> like raw data appends though
    // the offset is interval based instead of time based.
    final int type; // TODO make sure type < 126
    final int offset_start;
    if (qualifier.length < 6) {
      type = qualifier[0];
      offset_start = 1;
    } else {
      type = interval.rollupConfig().getIdForAggregator(qualifier);
      offset_start = interval.rollupConfig().getOffsetStartFromQualifier(qualifier);
    }
    
    byte[] data = (byte[]) pooled_array.object();
    int value_length = NumericCodec.getValueLengthFromQualifier(qualifier, offset_start);
    System.out.println("      EXP: " + value_length + "  GOT: " + value.length);
    if (value.length == value_length) {
      if (write_idx + 8 + 1 + 2 + value.length >= data.length) {
        new ReAllocatedArray();
        data = (byte[]) pooled_array.object();
      }
      
      int offset = (int) RollupUtils.getOffsetFromRollupQualifier(qualifier, offset_start, interval);
      if (offset < last_offset || 
          (offset == last_offset && last_type == (byte) type)) {
        needs_repair = true;
      }
      
      last_type = (byte) type;
      // TODO nanos and millis?
      
      System.out.println("       %%% Offset: " + offset + "  TS: " + ((base_timestamp.msEpoch() + offset) / 1000));
      
      Bytes.setLong(data, (base_timestamp.msEpoch() + offset) / 1000, write_idx);
      write_idx += 8;
      data[write_idx++] = 1;
      data[write_idx++] = (byte) type;
      
      if ((qualifier[offset_start] & NumericCodec.MS_BYTE_FLAG) == 
            NumericCodec.MS_BYTE_FLAG) {
        data[write_idx++] = NumericCodec.getFlags(qualifier, offset_start, (byte) NumericCodec.MS_Q_WIDTH);
      } else {
        data[write_idx++] = NumericCodec.getFlags(qualifier, offset_start, (byte) NumericCodec.S_Q_WIDTH);
      }
      System.arraycopy(value, 0, data, write_idx, value.length);
      write_idx += value.length;
    } else {
      // appended! So multiple offsets and values in the value array.
      // this has to be fast so we just dump data in, then we'll work on sorting
      // and deduping in that function.
      // TODO - how do we track order for diff types?
      for (int i = 0; i < value.length;) {
        value_length = NumericCodec.getValueLengthFromQualifier(value, i);
        if (write_idx + 8 + 1 + 2 + value.length >= data.length) {
          new ReAllocatedArray();
          data = (byte[]) pooled_array.object();
        }
        
        int offset = (int) RollupUtils.getOffsetFromRollupQualifier(value, offset_start, interval);
        if (offset < last_offset || 
            (offset == last_offset && last_type == (byte) type)) {
          needs_repair = true;
        }
        
        last_type = (byte) type;
        Bytes.setLong(data, base_timestamp.epoch() + (offset * interval.getIntervalSeconds()), write_idx);
        write_idx += 8;
        data[write_idx++] = 1;
        data[write_idx++] = (byte) type;
        
        if ((qualifier[offset_start] & NumericCodec.MS_BYTE_FLAG) == 
              NumericCodec.MS_BYTE_FLAG) {
          data[write_idx++] = NumericCodec.getFlags(qualifier, i, (byte) NumericCodec.MS_Q_WIDTH);
          i += NumericCodec.MS_Q_WIDTH;
        } else {
          data[write_idx++] = NumericCodec.getFlags(qualifier, i, (byte) NumericCodec.S_Q_WIDTH);
          i += NumericCodec.S_Q_WIDTH;
        }
        System.arraycopy(value, i, data, write_idx, value_length);
        write_idx += value_length;
        i += value_length;
      }
    }
    System.out.println(" (((( " + write_idx + " ))))");
  }
  
  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }

  @Override
  public long idHash() {
    return id_hash;
  }

  @Override
  public PartialTimeSeriesSet set() {
//    System.out.println("      responding with SET: " + set 
//        + "  FROM: " + System.identityHashCode(this));
    //new RuntimeException().printStackTrace();
    return set;
  }
//
//  @Override
//  public TypeToken<? extends TimeSeriesDataType> getType() {
//    return NumericByteArraySummaryType.TYPE;
//  }
  
  @Override
  public NumericByteArraySummaryType value() {
    return this;
  }
  
  @Override
  public void dedupe(final boolean keep_earliest, final boolean reverse) {
    if (pooled_array == null) {
      // no-op
      return;
    }
    
    if (!needs_repair) {
      // TODO - reverse
      return;
    }
    System.out.println("************* HAve to fix it......");
    // dedupe
    // TODO - any primitive tree maps out there? Or maybe there's just an
    // all around better way to do this. For now this should be fast enough. We
    // can't do an in-place swap easily since we have variable lengths (thank you
    // nanos!). Though there probably is an algo for that with an extra temp var.
    // The long is the offset from the base time, the map keys are the types and
    // the int values are the offset in the array. Just make it cleaner.
    TreeMap<Long, TreeMap<Byte, Integer>> map = new TreeMap<Long, TreeMap<Byte, Integer>>();
    int idx = 0;
    byte[] data = (byte[]) pooled_array.object();
    while (idx < write_idx) {
      long ts = Bytes.getInt(data, idx);
      TreeMap<Byte, Integer> summaries = map.get(ts);
      if (summaries == null) {
        summaries = Maps.newTreeMap();
        map.put(ts, summaries);
      }
      idx += 8;
      
      byte num_values = data[idx++];
      for (byte i = 0; i < num_values; i++) {
        if (keep_earliest) {
          summaries.putIfAbsent(data[idx], idx + 1);
        } else {
          summaries.put(data[idx], idx + 1);
        }
        
        idx += NumericCodec.getValueLengthFromQualifier(data, idx + 1);
      }
    }
    
    // TODO - see if we can checkout another array
    byte[] new_array = new byte[write_idx + 1];
    final Iterator<Entry<Long, TreeMap<Byte, Integer>>> outer_iterator;
    if (reverse) {
      outer_iterator = map.descendingMap().entrySet().iterator();
    } else {
      outer_iterator = map.entrySet().iterator();
    }
    
    idx = 0;
    while (outer_iterator.hasNext()) {
      final Entry<Long, TreeMap<Byte, Integer>> entry = outer_iterator.next();
      Bytes.setLong(new_array, entry.getKey(), idx);
      idx += 8;
      new_array[idx++] = (byte) entry.getValue().size();
      final Iterator<Entry<Byte, Integer>> inner_iterator = entry.getValue().entrySet().iterator();
      while (inner_iterator.hasNext()) {
        final Entry<Byte, Integer> value = inner_iterator.next();
        new_array[idx++] = value.getKey();
        final int len = NumericCodec.getValueLengthFromQualifier(data, value.getValue());
        new_array[idx++] = data[value.getValue()];
        System.arraycopy(data, value.getValue() + 1, new_array, idx, len);
        idx += len;
      }
    }
    
    // copy back
    write_idx = idx;
    System.arraycopy(new_array, 0, new_array, 0, idx);
    needs_repair = false;
  }
  
  @Override
  public boolean sameHash(final long hash) {
    if (write_idx <= 0) {
      return true;
    }
    
    return id_hash == hash;
  }
  
  /**
   * A simple class used when we exceed the size of the pooled array.
   */
  class ReAllocatedArray implements PooledObject {
    
    byte[] array;
    
    /**
     * Copies and adds 16 longs to the array.
     */
    ReAllocatedArray() {
      array = new byte[write_idx + 16];
      System.arraycopy(((byte[]) pooled_array.object()), 0, array, 0, write_idx);
      try {
        pooled_array.release();
      } catch (Throwable t) {
        LOG.error("Whoops, issue releasing pooled array", t);
      }
      pooled_array = this;
      // TODO - log this
    }
    
    @Override
    public Object object() {
      return array;
    }

    @Override
    public void release() {
      // no-op
    }
    
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return write_idx;
  }

  @Override
  public byte[] data() {
    if (pooled_array != null) {
      reference_counter.incrementAndGet();
      return (byte[]) pooled_array.object();
    } else {
      return null;
    }
  }
}

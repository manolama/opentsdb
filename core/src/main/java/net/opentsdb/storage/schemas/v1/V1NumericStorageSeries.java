package net.opentsdb.storage.schemas.v1;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.storage.StorageSeries;
import net.opentsdb.utils.Bytes;

public class V1NumericStorageSeries implements StorageSeries {
  
  /** Index's for the write and read paths over the array. */
  private int write_offset_idx;
  private int write_value_idx;
  
  /** The time offsets and real + value flags. */
  private byte[] offsets;
  
  /** The real counts and values. */
  private byte[] values;
  
  List<Integer> dummy_values = Lists.newArrayList();
  
  public V1NumericStorageSeries() {
  }

  @Override
  public Iterator<TimeSeriesValue<?>> iterator() {
    System.out.println("NEW ITERATOR!!!!!!!!!!!!");
    return new LocalIterator();
  }

  @Override
  public void optimize() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void decode(TimeStamp base, byte[] tsuid, byte prefix,
      byte[] qualifier, byte[] value) {
    // TODO - decode into row sequences
    // WARNING - need to sync
    
    // hacky hack
    dummy_values.add(dummy_values.size());
  }
  
  protected class LocalIterator implements Iterator<TimeSeriesValue<?>>,
    TimeSeriesValue<NumericType>, NumericType {

    Iterator<Integer> it = dummy_values.iterator();
    int value = 0; 
    long ts = 1519171200000L;
    @Override
    public TimeStamp timestamp() {
      return new MillisecondTimeStamp(ts += 60000);
    }

    @Override
    public NumericType value() {
      return this;
    }

    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public TimeSeriesValue<?> next() {
      value = it.next();
      return this;
    }

    @Override
    public boolean isInteger() {
      return true;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public double doubleValue() {
      return value;
    }

    @Override
    public double toDouble() {
      return value;
    }
    
  }
}

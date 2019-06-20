package net.opentsdb.data.types.numeric;

import java.util.Arrays;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeStamp;

public class MockNumericLongArrayTimeSeries implements 
    PartialTimeSeries<NumericLongArrayType>,
    NumericLongArrayType {

  private final PartialTimeSeriesSet set;
  private final long id_hash;
  private long[] data;
  private int write_idx;
  
  public MockNumericLongArrayTimeSeries(final PartialTimeSeriesSet set,
                                        final long id_hash) {
    this.set = set;
    this.id_hash = id_hash;
    data = new long[16];
  }
  
  public MockNumericLongArrayTimeSeries addValue(final int epoch, 
                                                 final long value) {
    if (write_idx + 2 >= data.length) {
      grow();
    }
    
    // no flags
    data[write_idx++] = epoch;
    data[write_idx++] = value;
    return this;
  }
  
  public MockNumericLongArrayTimeSeries addValue(final int epoch, 
                                                 final double value) {
    if (write_idx + 2 >= data.length) {
      grow();
    }
    
    // double
    data[write_idx++] = FLOAT_FLAG | (long) epoch;
    data[write_idx++] = Double.doubleToRawLongBits(value);
    return this;
  }
  
  public MockNumericLongArrayTimeSeries addValue(final long ms_epoch, 
                                                 final long value) {
    if (write_idx + 2 >= data.length) {
      grow();
    }
    
    data[write_idx++] = MILLISECOND_FLAG | ms_epoch;
    data[write_idx++] = value;
    return this;
  }
  
  public MockNumericLongArrayTimeSeries addValue(final long ms_epoch, 
                                                 final double value) {
    if (write_idx + 2 >= data.length) {
      grow();
    }
    
    // double
    data[write_idx++] = FLOAT_FLAG | MILLISECOND_FLAG & ms_epoch;
    data[write_idx++] = Double.doubleToRawLongBits(value);
    return this;
  }
  
  public MockNumericLongArrayTimeSeries addValue(final TimeStamp timestamp, 
                                                 final long value) {
    if (timestamp.nanos() > 0) {
      if (write_idx + 3 >= data.length) {
        grow();
      }
      
      data[write_idx++] = NANOSECOND_FLAG | timestamp.epoch();
      data[write_idx++] = timestamp.nanos();
    } else {
      data[write_idx++] = MILLISECOND_FLAG | timestamp.msEpoch();
    }
    data[write_idx++] = value;
    return this;
  }
  
  public MockNumericLongArrayTimeSeries addValue(final TimeStamp timestamp, 
                                                 final double value) {
    if (timestamp.nanos() > 0) {
      if (write_idx + 3 >= data.length) {
        grow();
      }
      
      data[write_idx++] = FLOAT_FLAG | NANOSECOND_FLAG | timestamp.epoch();
      data[write_idx++] = timestamp.nanos();
    } else {
      data[write_idx++] = FLOAT_FLAG | MILLISECOND_FLAG | timestamp.msEpoch();
    }
    data[write_idx++] = Double.doubleToRawLongBits(value);
    return this;
  }
  
  @Override
  public void close() throws Exception {
  }

  @Override
  public long idHash() {
    return id_hash;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }

  @Override
  public TimeSeriesDataType value() {
    return this;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericLongArrayType.TYPE;
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
  public long[] data() {
    return data;
  }

  void grow() {
    data = Arrays.copyOf(data, data.length + 16);
  }
}

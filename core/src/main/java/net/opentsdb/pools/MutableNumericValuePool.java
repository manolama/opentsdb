package net.opentsdb.pools;

import java.util.concurrent.TimeUnit;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericType;
import stormpot.BlazePool;
import stormpot.Slot;

public class MutableNumericValuePool extends StormPotPool {

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = id;
    registerConfigs(tsdb.getConfig());
    
    allocator = new MNVAllocator();
    System.out.println("******** LOADED ALLOC: " + allocator + " KEY: " + myConfig(ALLOCATOR_KEY));
    if (allocator == null) {
      return Deferred.fromError(new IllegalArgumentException("No allocator found for: " + myConfig(ALLOCATOR_KEY)));
    }
    
    stormpot.Config<MyPoolable> config = 
        new stormpot.Config<MyPoolable>()
        .setAllocator(new MyAllocator())
        .setSize(tsdb.getConfig().getInt(myConfig(INITIAL_COUNT_KEY)));
    stormpot = new BlazePool<MyPoolable>(config);
    default_timeout = new stormpot.Timeout(1, TimeUnit.NANOSECONDS);
    return Deferred.fromResult(null);
  }
  
  public static class PooledMutableNumericValue implements Poolable, stormpot.Poolable, NumericType, 
      TimeSeriesValue<NumericType>, AutoCloseable {

    private Slot slot;
    
    //NOTE: Fields are not final to make an instance available to store a new
    // pair of a timestamp and a value to reduce memory burden.
    
    /** The timestamp for this data point. */
    private TimeStamp timestamp;
    
    /** True if the value is stored as a long. */
    private boolean is_integer = true;
    
    /** A long value or a double encoded on a long if {@code is_integer} is false. */
    private long value = 0;
    
    /** Whether or not the current value is null. */
    private boolean nulled; 
    
    /**
     * Initialize a new mutable data point with a {@link Long} value of 0.
     */
    protected PooledMutableNumericValue() {
      nulled = false;
      timestamp = new MillisecondTimeStamp(0);
    }
    
    @Override
    public void release() {
      if (slot != null) {
        slot.release(this);
      }
    }
    
    @Override
    public boolean isInteger() {
      return is_integer;
    }

    @Override
    public long longValue() {
      if (is_integer) {
        return value;
      }
      throw new ClassCastException("Not a long in " + toString());
    }

    @Override
    public double doubleValue() {
      if (!is_integer) {
        return Double.longBitsToDouble(value);
      }
      throw new ClassCastException("Not a double in " + toString());
    }

    @Override
    public double toDouble() {
      if (is_integer) {
        return value;
      }
      return Double.longBitsToDouble(value);
    }
    
    @Override
    public TimeStamp timestamp() {
      return timestamp;
    }

    @Override
    public NumericType value() {
      return nulled ? null : this;
    }
    
    /**
     * Reset the value given the timestamp and value.
     * @param timestamp A non-null timestamp.
     * @param value A numeric value.
     * @throws IllegalArgumentException if the timestamp was null.
     */
    public void reset(final TimeStamp timestamp, final long value) {
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null");
      }
      if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = timestamp.getCopy();
      } else {
        this.timestamp.update(timestamp);
      }
      this.value = value;
      is_integer = true;
      nulled = false;
    }
    
    /**
     * Reset the value given the timestamp and value.
     * @param timestamp A non-null timestamp.
     * @param value A numeric value.
     * @throws IllegalArgumentException if the timestamp was null.
     */
    public void reset(final TimeStamp timestamp, final double value) {
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null");
      }
      if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = timestamp.getCopy();
      } else {
        this.timestamp.update(timestamp);
      }
      this.value = Double.doubleToRawLongBits(value);
      is_integer = false;
      nulled = false;
    }
    
    /**
     * Resets the local value by copying the timestamp and value from the source
     * value.
     * @param value A non-null value.
     * @throws IllegalArgumentException if the value was null or the value's 
     * timestamp was null.
     */
    public void reset(final TimeSeriesValue<NumericType> value) {
      if (value == null) {
        throw new IllegalArgumentException("Value cannot be null");
      }
      if (value.timestamp() == null) {
        throw new IllegalArgumentException("Value's timestamp cannot be null");
      }
      if (value.timestamp().units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = value.timestamp().getCopy();
      } else {
        this.timestamp.update(value.timestamp());
      }
      if (value.value() != null) {
        this.value = value.value().isInteger() ? value.value().longValue() : 
          Double.doubleToRawLongBits(value.value().doubleValue());
        is_integer = value.value().isInteger();
        nulled = false;
      } else {
        nulled = true;
      }
    }

    /**
     * Resets the local value by copying the timestamp and value from the arguments.
     * @param A non-null timestamp.
     * @param value A numeric value.
     * @throws IllegalArgumentException if the timestamp or value was null.
     */
    public void reset(final TimeStamp timestamp, final NumericType value) {
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null.");
      }
      if (value == null) {
        throw new IllegalArgumentException("Value cannot be null.");
      }
      if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = timestamp.getCopy();
      } else {
        this.timestamp.update(timestamp);
      }
      if (value.isInteger()) {
        is_integer = true;
        this.value = value.longValue();
      } else {
        is_integer = false;
        this.value = Double.doubleToRawLongBits(value.doubleValue());
      }
      nulled = false;
    }
    
    /**
     * Resets the value to null with the given timestamp.
     * @param timestamp A non-null timestamp to update from.
     * @throws IllegalArgumentException if the timestamp was null.
     */
    public void resetNull(final TimeStamp timestamp) {
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null.");
      }
      if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = timestamp.getCopy();
      } else {
        this.timestamp.update(timestamp);
      }
      nulled = true;
    }
    
    /**
     * Resets just the value of the data point. Good for use with aggregators
     * but be CAREFUL not to return without updating the timestamp.
     * @param value The value to set.
     */
    public void resetValue(final long value) {
      this.value = value;
      is_integer = true;
      nulled = false;
    }
    
    /**
     * Resets just the value of the data point. Good for use with aggregators
     * but be CAREFUL not to return without updating the timestamp.
     * @param value The value to set.
     */
    public void resetValue(final double value) {
      this.value = Double.doubleToRawLongBits(value);
      is_integer = false;
      nulled = false;
    }
    
    /**
     * Resets just the timestamp of the data point. Good for use with 
     * aggregators but be CAREFUL not to return without updating the value.
     * @param timestamp The timestamp to set.
     */
    public void resetTimestamp(final TimeStamp timestamp) {
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null");
      }
      if (timestamp.units().ordinal() < this.timestamp.units().ordinal()) {
        this.timestamp = timestamp.getCopy();
      } else {
        this.timestamp.update(timestamp);
      }
    }
    
    @Override
    public TypeToken<NumericType> type() {
      return NumericType.TYPE;
    }

    @Override
    public String toString() {
      final StringBuilder buf = new StringBuilder()
          .append("timestamp=")
          .append(timestamp)
          .append(", nulled=")
          .append(nulled)
          .append(", isInteger=")
          .append(isInteger())
          .append(", value=");
      if (isInteger()) {
        buf.append(longValue());
      } else {
        buf.append(doubleValue());
      }
      return buf.toString();
          
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof PooledMutableNumericValue)) {
        return false;
      }

      PooledMutableNumericValue that = (PooledMutableNumericValue) obj;

      if (this.nulled) {
        return false;
      } else {
        return this.timestamp.compare(Op.EQ, that.timestamp) && this.value == that.value
            && this.is_integer == that.is_integer && this.nulled == that.nulled;
      }
    }

    @Override
    public void close() throws Exception {
      release();
    }

    @Override
    public Object object() {
      return this;
    }

  }
  
  public static class MNVAllocator extends BaseTSDBPlugin implements stormpot.Allocator<PooledMutableNumericValue>, Allocator {

    @Override
    public PooledMutableNumericValue allocate(Slot slot) throws Exception {
      PooledMutableNumericValue v = new PooledMutableNumericValue();
      v.slot = slot;
      return v;
    }

    @Override
    public void deallocate(PooledMutableNumericValue poolable)
        throws Exception {
      poolable.release();
    }

    @Override
    public int size() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public Object allocate() {
      return new PooledMutableNumericValue();
    }

    @Override
    public void deallocate(Object object) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public TypeToken<?> dataType() {
      // TODO Auto-generated method stub
      return TypeToken.of(PooledMutableNumericValue.class);
    }

    @Override
    public String type() {
      // TODO Auto-generated method stub
      return "MNVAllocator";
    }

    @Override
    public String version() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public Deferred<Object> initialize(TSDB tsdb, String id) {
      this.id = id;
      return Deferred.fromResult(null);
    }
  }
  
}

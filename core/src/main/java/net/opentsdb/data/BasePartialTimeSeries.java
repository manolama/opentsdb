package net.opentsdb.data;

import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;

public abstract class BasePartialTimeSeries<T extends TimeSeriesDataType> 
  implements PartialTimeSeries<T>, CloseablePooledObject  {

  /** Reference to the Object pool for this instance. */
  protected PooledObject pooled_object;
  
  /** A hash for the time series ID. */
  protected long id_hash;
  
  /** The set we currently belong to. */
  protected PartialTimeSeriesSet set;
  
  /** A reference counter for the array to determine when we can return it to
   * the pool. */
  protected AtomicInteger reference_counter;

  protected BasePartialTimeSeries() {
    reference_counter = new AtomicInteger();
  }
  
  @Override
  public void close() throws Exception {
    final int ref = reference_counter.decrementAndGet();
    if (ref == 0) {
      release();
    }
  }
  
  @Override
  public long idHash() {
    return id_hash;
  }

  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }
  
  @Override
  public Object object() {
    return this;
  }

  protected void baseRelease() {
    set = null;
    reference_counter.set(0);
    pooled_object.release();
  }
  
  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }
  
}

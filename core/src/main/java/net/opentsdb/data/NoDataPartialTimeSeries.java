package net.opentsdb.data;

import com.google.common.reflect.TypeToken;

import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.PooledObject;

public class NoDataPartialTimeSeries implements PartialTimeSeries, CloseablePooledObject {

  private PooledObject pooled_object;
  
  private PartialTimeSeriesSet set;
  
  public void reset(final PartialTimeSeriesSet set) {
    this.set = set;
  }
  
  @Override
  public void close() throws Exception {
    release();
  }

  @Override
  public long idHash() {
    return 0;
  }

  @Override
  public PartialTimeSeriesSet set() {
    return set;
  }

  @Override
  public TypeToken<? extends TimeSeriesDataType> getType() {
    return NoDataType.TYPE;
  }

  @Override
  public Object data() {
    return null;
  }

  @Override
  public Object object() {
    return this;
  }

  @Override
  public void release() {
    if (pooled_object != null) {
      pooled_object.release();
    }
  }

  @Override
  public void setPooledObject(final PooledObject pooled_object) {
    this.pooled_object = pooled_object;
  }

}

package net.opentsdb.storage.schemas.v1;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesDataId;
import net.opentsdb.data.TimeSeriesId;

public class V1TimeSeriesId implements TimeSeriesDataId {

  final byte[] tsuid;
  
  public V1TimeSeriesId(final byte[] tsuid) {
    this.tsuid = tsuid;
  }
  
  @Override
  public boolean encoded() {
    return true;
  }
  
  @Override
  public boolean decodeToJoin() {
    // TODO Auto-generated method stub
    return false;
  }
  
  @Override
  public Deferred<TimeSeriesId> decode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(TimeSeriesDataId o) {
    // TODO Auto-generated method stub
    return 0;
  }

}

package net.opentsdb.storage.schemas.v1;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.storage.StorageSchema;
import net.opentsdb.utils.Bytes.ByteMap;

public class V1TimeSeriesId implements TimeSeriesByteId {

  final byte[] tsuid;
  
  public V1TimeSeriesId(final byte[] tsuid) {
    this.tsuid = tsuid;
  }

  @Override
  public boolean encoded() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean decodeToJoin() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Deferred<TimeSeriesStringId> decode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(TimeSeriesByteId o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public StorageSchema schema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] alias() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] namespace() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] metric() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] tags() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] aggregatedTags() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] disjointTags() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] timeseriesUID() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] tagValue(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }
  
}

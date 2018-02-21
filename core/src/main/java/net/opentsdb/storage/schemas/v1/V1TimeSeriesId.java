package net.opentsdb.storage.schemas.v1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.storage.StorageSchema;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Bytes.ByteMap;

public class V1TimeSeriesId implements TimeSeriesByteId {

  final byte[] tsuid;
  
  final UniqueIdStore uid_store;
  
  public V1TimeSeriesId(final byte[] tsuid, final UniqueIdStore uid_store) {
    this.tsuid = tsuid;
    this.uid_store = uid_store;
  }

  @Override
  public boolean encoded() {
    return true;
  }

  @Override
  public boolean decodeToJoin() {
    // TODO - fix me
    return false;
  }

  @Override
  public Deferred<TimeSeriesStringId> decode() {
System.out.println("RESOLVING ID!");
    final BaseTimeSeriesId.Builder builder = BaseTimeSeriesId.newBuilder();
    final String[] tag_keys = new String[1]; // TODO - proper length
    final String[] tag_values = new String[1]; // TODO - proper length
    
    try {
    class FinalCB implements Callback<TimeSeriesStringId, ArrayList<Object>> {

      @Override
      public TimeSeriesStringId call(ArrayList<Object> ignored) throws Exception {
        System.out.println("GOT THE FINAL RESOLVE!");
        for (int i = 0; i < tag_keys.length; i++) {
          builder.addTags(tag_keys[i], tag_values[i]);
        }
        return builder.build();
      }
      
    }
    
    class TagKeyCB implements Callback<Object, List<String>> {

      @Override
      public Object call(List<String> arg0) throws Exception {
        for (int i = 0; i < arg0.size(); i++) {
          tag_keys[i] = arg0.get(i);
        }
        return null;
      }
      
    }
    
    class TagValueCB implements Callback<Object, List<String>> {

      @Override
      public Object call(List<String> arg0) throws Exception {
        for (int i = 0; i < arg0.size(); i++) {
          tag_values[i] = arg0.get(i);
        }
        return null;
      }
      
    }
    
    class MetricCB implements Callback<Object, String> {

      @Override
      public Object call(final String metric) throws Exception {
        builder.setMetric(metric);
        return null;
      }
      
    }
    
    List<Deferred<Object>> deferreds = Lists.newArrayListWithCapacity(3);
    
    deferreds.add(uid_store.idToString(UniqueIdType.METRIC, Arrays.copyOfRange(tsuid, 0, 3)) // TODO - fix me
        .addCallback(new MetricCB()));
    
    List<byte[]> ids = Lists.newArrayListWithCapacity(1); // TODO  - proper size
    for (int i = 3; i < tsuid.length; i += 6) {
      ids.add(Arrays.copyOfRange(tsuid, i, i + 3));
    }
    deferreds.add(uid_store.idsToString(UniqueIdType.TAGK, ids)
        .addCallback(new TagKeyCB()));
    
    ids = Lists.newArrayListWithCapacity(1); // TODO  - proper size
    for (int i = 6; i < tsuid.length; i += 6) {
      ids.add(Arrays.copyOfRange(tsuid, i, i + 3));
    }
    deferreds.add(uid_store.idsToString(UniqueIdType.TAGV, ids)
        .addCallback(new TagValueCB()));
    
    System.out.println("RETURNING from the resolution...");
    return Deferred.group(deferreds).addCallback(new FinalCB());
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public int compareTo(final TimeSeriesByteId o) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("IMPLEMENT ME!");
  }

  @Override
  public StorageSchema schema() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("IMPLEMENT ME!");
  }

  @Override
  public byte[] alias() {
    return null;
  }

  @Override
  public byte[] namespace() {
    return null;
  }

  @Override
  public byte[] metric() {
    // TODO Auto-generated method stub
    return Arrays.copyOfRange(tsuid, 0, 3);
  }

  @Override
  public byte[] tags() {
    return Arrays.copyOfRange(tsuid, 3, tsuid.length);
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
    return tsuid;
  }

  @Override
  public byte[] tagValue(byte[] key) {
    // TODO make sure key length is correct
    int idx = 3;
    while (idx < key.length) {
      boolean matched = true;
      for (int i = 0; i < 3; i++) {
        if (key[i] != tsuid[idx + i]) {
          matched = false;
          break;
        }
      }
      if (matched) {
        return Arrays.copyOfRange(tsuid, idx + 3, idx + 6);
      }
      
      idx += 6;
    }
    return null;
  }
  
}

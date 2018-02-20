package net.opentsdb.uid;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.UniqueId.UniqueIdType;

public interface UniqueIdStore {

  public Deferred<byte[]> stringToId(final UniqueIdType type, final String id);
  
  public Deferred<List<byte[]>> stringsToId(final UniqueIdType type, final List<String> ids);
  
  public Deferred<String> idToString(final UniqueIdType type, final byte[] id);
  
  public Deferred<List<String>> idsToString(final UniqueIdType type, final List<byte[]> ids);
}

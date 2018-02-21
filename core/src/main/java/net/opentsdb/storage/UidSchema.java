package net.opentsdb.storage;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.uid.ResolvedFilter;
import net.opentsdb.uid.UniqueId.UniqueIdType;

public interface UidSchema extends StorageSchema {

  public byte[] timelessKey(final byte[] key);
  
  public TimeStamp baseTimestamp(final byte[] key);
  
  public int uidWidth(final UniqueIdType type);
  
  public Deferred<List<ResolvedFilter>> resolveUids(final Filter filter);
  
  public Deferred<byte[]> stringToId(final UniqueIdType type, final String id);
  
  public Deferred<List<byte[]>> stringsToId(final UniqueIdType type, final List<String> ids);
  
  public Deferred<String> idToString(final UniqueIdType type, final byte[] id);
  
  public Deferred<List<String>> idsToString(final UniqueIdType type, final List<byte[]> ids);
}

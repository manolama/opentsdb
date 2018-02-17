package net.opentsdb.storage;

import java.util.Collection;

import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Deferred;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;

public class UniqueIds {

  private final HBaseClient client;
  
  private final byte[] table;
  
  private final byte[] family;
  
  private UniqueId metrics;
  
  private UniqueId tag_keys;
  
  private UniqueId tag_values;
  
  public UniqueIds(final HBaseClient client) {
    this.client = client;
    
    // TODO - create the UniqueIds
    table = null;
    family = null;
  }
  
  public Deferred<byte[]> resolveUid(final UniqueIdType type, final String name) {
    return null;
  }
  
  public Deferred<byte[][]> resolveUids(final UniqueIdType type, final Collection<String> names) {
    return null;
  }
}

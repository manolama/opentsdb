package net.opentsdb.storage;

import net.opentsdb.data.TimeStamp;

public interface UidSchema extends StorageSchema {

  public byte[] timelessKey(final byte[] key);
  
  public TimeStamp baseTimestamp(final byte[] key);
}

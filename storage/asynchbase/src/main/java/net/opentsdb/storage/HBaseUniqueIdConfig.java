package net.opentsdb.storage;

import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.uid.UniqueIdConfig;

public class HBaseUniqueIdConfig extends UniqueIdConfig {

  public HBaseUniqueIdConfig(Mode mode, UniqueIdType type, int width) {
    super(mode, type, width);
    // TODO Auto-generated constructor stub
  }

  public byte[] table() {
    // TODO - implement!
    return null;
  }
  
  public byte[] idFamily() {
    // TODO - implement!
    return null;
  }
  
  public byte[] nameFamily() {
    // TODO - implement!
    return null;
  }
}

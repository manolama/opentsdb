package net.opentsdb.storage;

/**
 * Just a simple class that is used to hold the state of long running/paged
 * queries to the storage system. The implementation layer will overload 
 * Scanner for their specific platform.
 */
public class StorageQuery {
  
  /** Represents where the scanner should start */
  public byte[] start;
  
  /** Represents when the scanner should stop */
  public byte[] end;
  
  /** 
   * Scanner set by the storage implementer, the caller will simply see if
   * this is null and call hasNext()
   */
  public Scanner scanner = null;
}

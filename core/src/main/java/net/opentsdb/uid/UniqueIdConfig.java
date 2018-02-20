package net.opentsdb.uid;

import java.nio.charset.Charset;

import net.opentsdb.uid.UniqueId.UniqueIdType;

public class UniqueIdConfig {

  public enum Mode {
    WRITE_ONLY, // populate only the string to ID cache
    READ_ONLY, // populate only the id to string cache 
    READ_WRITE // populate both caches
  }
  
  public Mode mode() {
    // TODO - implement!
    return Mode.READ_WRITE;
  }
  
  public UniqueIdType type() {
    // TODO - implement!
    return UniqueIdType.METRIC;
  }
  
  public int width() {
    // TODO - implement!
    return 3;
  }
  
  public Charset characterSet() {
    // TODO - implement!
    return Charset.forName("ISO-8859-1");
  }
  
  public int maxAssignmentAttempts() {
    // TODO - implement!    
    return 1;
  }
  
  // OOooo useful!
  public void allowedCharacters() {
    
  }
  
}

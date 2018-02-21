package net.opentsdb.uid;

import java.nio.charset.Charset;

import net.opentsdb.uid.UniqueId.UniqueIdType;

public class UniqueIdConfig {

  public enum Mode {
    WRITE_ONLY, // populate only the string to ID cache
    READ_ONLY, // populate only the id to string cache 
    READ_WRITE // populate both caches
  }
  
  public Mode mode;
  public UniqueIdType type;
  public int width;
  public Charset charset = Charset.forName("ISO-8859-1");
  
  public UniqueIdConfig(final Mode mode, final UniqueIdType type, final int width) {
    this.mode = mode;
    this.type = type;
    this.width = width;
  }
  
  public Mode mode() {
    return mode;
  }
  
  public UniqueIdType type() {
    return type;
  }
  
  public int width() {
    return width;
  }
  
  public Charset characterSet() {
    return charset;
  }
  
  public int maxAssignmentAttempts() {
    // TODO - implement!    
    return 1;
  }
  
  // OOooo useful!
  public void allowedCharacters() {
    
  }
  
}

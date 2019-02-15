package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import net.opentsdb.core.TSDBPlugin;

public interface ObjectPool extends TSDBPlugin {

  public Poolable claim();
  
  public Poolable claim(final long time, final ChronoUnit unit);
  
  public interface Poolable {
    
    public Object object();
    
    public void release();
    
  }
  
  public static abstract class Builder {
    
  }
}

package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import net.opentsdb.core.TSDBPlugin;

public interface ObjectPool extends TSDBPlugin {

  public static final String PREFIX = "objectpool.";
  public static final String ALLOCATOR_KEY = "allocator";
  public static final String INITIAL_COUNT_KEY = "count.initial";
  
  public Poolable claim();
  
  public Poolable claim(final long time, final ChronoUnit unit);
  
  public interface Poolable {
    
    public Object object();
    
    public void release();
    
  }
  
  public static abstract class Builder {
    
  }
}

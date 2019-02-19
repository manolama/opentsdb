package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDBPlugin;

public interface ObjectPool {

  public Poolable claim();
  
  public Poolable claim(final long time, final ChronoUnit unit);
  
  public int overhead();
  
  public String id();
  
  public Deferred<Object> shutdown();
  
  public interface Poolable {
    
    public Object object();
    
    public void release();
    
  }
  
  public interface PoolConfig {
    public Allocator getAllocator();
    public int initialCount();
    public int maxCount();
    public String id();
  }
  
}

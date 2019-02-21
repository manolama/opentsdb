package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;

import com.stumbleupon.async.Deferred;

public interface ObjectPool {

  public Poolable claim();
  
  public Poolable claim(final long time, final ChronoUnit unit);
  
  public int overhead();
  
  public String id();
  
  public Deferred<Object> shutdown();
  
}

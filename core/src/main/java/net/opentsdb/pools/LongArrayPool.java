package net.opentsdb.pools;

import java.util.concurrent.TimeUnit;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import stormpot.BlazePool;

public class LongArrayPool extends StormPotPool {

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = id;
    registerConfigs(tsdb.getConfig());
    
    allocator = tsdb.getRegistry().getPlugin(Allocator.class, "LongArrayAllocator");
    System.out.println("******** LOADED ALLOC: " + allocator + " KEY: " + myConfig(ALLOCATOR_KEY));
    if (allocator == null) {
      return Deferred.fromError(new IllegalArgumentException("No allocator found for: " + myConfig(ALLOCATOR_KEY)));
    }
    
    stormpot.Config<MyPoolable> config = 
        new stormpot.Config<MyPoolable>()
        .setAllocator(new MyAllocator())
        .setSize(tsdb.getConfig().getInt(myConfig(INITIAL_COUNT_KEY)));
    stormpot = new BlazePool<MyPoolable>(config);
    default_timeout = new stormpot.Timeout(1, TimeUnit.NANOSECONDS);
    return Deferred.fromResult(null);
  }
  
}

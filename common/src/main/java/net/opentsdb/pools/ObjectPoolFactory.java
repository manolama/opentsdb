package net.opentsdb.pools;

import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.pools.ObjectPool.PoolConfig;

public interface ObjectPoolFactory extends TSDBPlugin {

  public static final String PREFIX = "objectpool.";
  public static final String ALLOCATOR_KEY = "allocator";
  public static final String INITIAL_COUNT_KEY = "count.initial";
  
  public ObjectPool newPool(final PoolConfig config);
  
}

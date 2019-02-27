package net.opentsdb.pools;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

public class LongArrayPool implements Allocator {
  public static final String TYPE = "LongArrayPool";
  private static final TypeToken<?> TYPE_TOKEN = TypeToken.of(long[].class);
  private int length;
  private String id;
  private int size;
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    final String key = "pool.allocator." + (Strings.isNullOrEmpty(id) ? "" : id) 
        + ".primitive.array.length";
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    if (!tsdb.getConfig().hasProperty(key)) {
      tsdb.getConfig().register(key, 4096, false, 
          "The length of each array to allocate");
    }
    length = tsdb.getConfig().getInt(key);
    size = (8 * length) +  + 16 /* 64-bit overhead */;
    
    final ObjectPoolFactory factory = 
        tsdb.getRegistry().getPlugin(ObjectPoolFactory.class, null);
    if (factory == null) {
      return Deferred.fromError(new RuntimeException("No default pool factory found."));
    }
    
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(tsdb.getConfig().getInt(key))
        .setId(this.id)
        .build();
    
    final ObjectPool pool = factory.newPool(config);
    if (pool != null) {
      tsdb.getRegistry().registerObjectPool(pool);
    } else {
      return Deferred.fromError(new RuntimeException("Null pool returned for: " + id));
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public Object allocate() {
    return new long[length];
  }

  @Override
  public void deallocate(final Object object) {
    // no-op
  }

  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }
  
}

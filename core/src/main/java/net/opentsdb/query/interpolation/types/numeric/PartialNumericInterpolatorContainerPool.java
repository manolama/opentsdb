package net.opentsdb.query.interpolation.types.numeric;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.pools.Allocator;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolConfig;
import net.opentsdb.pools.ObjectPoolFactory;

public class PartialNumericInterpolatorContainerPool implements Allocator {
  public static final String TYPE = "PartialNumericInterpolatorContainerPool";
  private static final TypeToken<?> TYPE_TOKEN = 
      TypeToken.of(PartialNumericInterpolatorContainer.class);
  private int length;
  private String id;
  private int size;
  private TSDB tsdb;
  
  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.tsdb = tsdb;
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    final ObjectPoolFactory factory = 
        tsdb.getRegistry().getPlugin(ObjectPoolFactory.class, null);
    if (factory == null) {
      return Deferred.fromError(new RuntimeException("No default pool factory found."));
    }
    
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(4096) // TODO
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Object allocate() {
    // TODO Auto-generated method stub
    return new PartialNumericInterpolatorContainer(tsdb);
  }

  @Override
  public void deallocate(Object object) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public TypeToken<?> dataType() {
    // TODO Auto-generated method stub
    return TYPE_TOKEN;
  }

}

package net.opentsdb.pools;

import org.openjdk.jol.info.ClassLayout;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.MutableNumericValue;

public class MutableNumericValuePool implements Allocator {
  public static final String TYPE = "MutableNumericValuePool";
  private static final TypeToken<?> TYPE_TOKEN = TypeToken.of(MutableNumericValue.class);
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
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    size = (int) ClassLayout.parseClass(MutableNumericValue.class).instanceSize();
    
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
    return "3.0.0";
  }

  @Override
  public int size() {
    return size;
  }
  
  @Override
  public Object allocate() {
    return new MutableNumericValue();
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

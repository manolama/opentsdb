package net.opentsdb.pools;

import org.openjdk.jol.info.ClassLayout;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.MutableNumericValue;

public class MutableNumericValueAllocator implements Allocator {
  private static final String TYPE = "MutableNumericValueAllocator";
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

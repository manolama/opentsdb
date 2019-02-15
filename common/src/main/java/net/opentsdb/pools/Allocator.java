package net.opentsdb.pools;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDBPlugin;

public interface Allocator extends TSDBPlugin {
  public int size();
  public Object allocate();
  public void deallocate(final Object object);
  public TypeToken<?> dataType();
}

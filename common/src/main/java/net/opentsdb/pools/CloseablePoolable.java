package net.opentsdb.pools;

public interface CloseablePoolable extends AutoCloseable {

  public void setPoolable(final Poolable poolable);
  
}

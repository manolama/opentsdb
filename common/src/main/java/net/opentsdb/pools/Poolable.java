package net.opentsdb.pools;

public interface Poolable {
  public Object object();
  
  public void release();
}

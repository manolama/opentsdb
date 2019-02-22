package net.opentsdb.pools;

public interface Poolable {
  public Object object();
  
  public void release();
  
  // positive or negative. 
  public void updateSize(final int size);
}

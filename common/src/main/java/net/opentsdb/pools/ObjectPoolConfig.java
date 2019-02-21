package net.opentsdb.pools;

public interface ObjectPoolConfig {
  public Allocator getAllocator();
  public int initialCount();
  public int maxCount();
  public int maxSize();
  public String id();
}

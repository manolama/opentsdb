package net.opentsdb.pools;

import com.google.common.base.Strings;

public abstract class BaseObjectPoolConfig implements ObjectPoolConfig {
  protected final Allocator allocator;
  protected final int initial_count;
  protected final int max_count;
  protected final int max_size;
  protected final String id;
  
  protected BaseObjectPoolConfig(final Builder builder) {
    if (builder.allocator == null) {
      throw new IllegalArgumentException("Allocator cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    
    allocator = builder.allocator;
    initial_count = builder.initial_count;
    max_count = builder.max_count;
    max_size = builder.max_size;
    id = builder.id;
  }
  
  @Override
  public Allocator getAllocator() {
    return allocator;
  }
  
  @Override
  public int initialCount() {
    return initial_count;
  }
  
  @Override
  public int maxCount() {
    return max_count;
  }
  
  @Override
  public int maxSize() {
    return max_size;
  }
  
  @Override
  public String id() {
    return id;
  }
  
  public static abstract class Builder {
    protected Allocator allocator;
    protected int initial_count;
    protected int max_count;
    protected int max_size;
    protected String id;
    
    public Builder setAllocator(final Allocator allocator) {
      this.allocator = allocator;
      return this;
    }
    
    public Builder setInitialCount(final int initial_count) {
      this.initial_count = initial_count;
      return this;
    }
    
    public Builder setMaxCount(final int max_count) {
      this.max_count = max_count;
      return this;
    }
    
    public Builder setMaxSize(final int max_size) {
      this.max_size = max_size;
      return this;
    }
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public abstract ObjectPoolConfig build();
  }
}

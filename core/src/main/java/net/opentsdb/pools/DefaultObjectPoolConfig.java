package net.opentsdb.pools;

public class DefaultObjectPoolConfig extends BaseObjectPoolConfig {

  protected DefaultObjectPoolConfig(Builder builder) {
    super(builder);
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder extends BaseObjectPoolConfig.Builder {

    @Override
    public ObjectPoolConfig build() {
      return new DefaultObjectPoolConfig(this);
    }
    
  }
}

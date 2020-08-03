package net.opentsdb.query.readcache;

import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.processor.topn.TopNConfig.Builder;

public class ReadCacheNodeConfig extends BaseQueryNodeConfig {

  protected ReadCacheNodeConfig(Builder builder) {
    super(builder);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean pushDown() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean joins() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public net.opentsdb.query.QueryNodeConfig.Builder toBuilder() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

  /** @return A new builder to work from. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder extends BaseQueryNodeConfig.Builder<Builder, ReadCacheNodeConfig> {
    Builder() {
      setType("TODO!");
    }
    
    @Override
    public ReadCacheNodeConfig build() {
      return new ReadCacheNodeConfig(this);
    }
    
    @Override
    public Builder self() {
      return this;
    }
  }
}

package net.opentsdb.query.filter;

import com.google.common.base.Strings;

public class DefaultNamedFilter implements NamedFilter {

  protected final String id;
  
  protected final QueryFilter filter;
  
  protected DefaultNamedFilter(final Builder builder) {
    if (Strings.isNullOrEmpty(builder.id)) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    if (builder.filter == null) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    id = builder.id;
    filter = builder.filter;
  }
  
  @Override
  public String id() {
    return id;
  }

  @Override
  public QueryFilter filter() {
    return filter;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private String id;
    private QueryFilter filter;
    
    public Builder setId(final String id) {
      this.id = id;
      return this;
    }
    
    public Builder setFilter(final QueryFilter filter) {
      this.filter = filter;
      return this;
    }
    
    public DefaultNamedFilter build() {
      return new DefaultNamedFilter(this);
    }
  }
}

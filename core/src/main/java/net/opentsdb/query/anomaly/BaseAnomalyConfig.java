package net.opentsdb.query.anomaly;

import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;

public abstract class BaseAnomalyConfig 
    extends BaseQueryNodeConfigWithInterpolators 
    implements AnomalyConfig {

  protected final ExecutionMode mode;
  protected final boolean serialize_observed;
  protected final boolean serialize_thresholds;
  
  protected BaseAnomalyConfig(final Builder builder) {
    super(builder);
    mode = builder.mode;
    serialize_observed = builder.serializeObserved;
    serialize_thresholds = builder.serializeThresholds;
  }
  
  @Override
  public ExecutionMode getMode() {
    return mode;
  }
  
  @Override
  public boolean getSerializeObserved() {
    return serialize_observed;
  }
  
  @Override
  public boolean getSerializeThresholds() {
    return serialize_thresholds;
  }
  
  public static abstract class Builder<B extends Builder<B, C>, 
                                       C extends BaseQueryNodeConfigWithInterpolators> 
      extends BaseQueryNodeConfigWithInterpolators.Builder<B, C> {
    protected ExecutionMode mode;
    protected boolean serializeObserved;
    protected boolean serializeThresholds;
    
    public B setMode(final ExecutionMode mode) {
      this.mode = mode;
      return self();
    }
    
    public B setSerializeObserved(final boolean serialize_observed) {
      serializeObserved = serialize_observed;
      return self();
    }
    
    public B setSerializeThresholds(final boolean serialize_thresholds) {
      serializeThresholds = serialize_thresholds;
      return self();
    }
  }
}

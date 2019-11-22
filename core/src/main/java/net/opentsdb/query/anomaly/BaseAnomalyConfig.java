package net.opentsdb.query.anomaly;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;

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
    
    public static void parseConfig(final ObjectMapper mapper, 
        final TSDB tsdb,
        final JsonNode node,
        final Builder builder) {
      JsonNode n = node.get("id");
      if (n != null) {
        builder.setId(n.asText());
      }
      
      n = node.get("mode");
      if (n != null && !n.isNull()) {
        builder.setMode(ExecutionMode.valueOf(n.asText()));
      }
      
      n = node.get("sources");
      if (n != null && !n.isNull()) {
        try {
          builder.setSources(mapper.treeToValue(n, List.class));
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException("Failed to parse json", e);
        }
      }
      
      n = node.get("interpolatorConfigs");
      for (final JsonNode config : n) {
        JsonNode type_json = config.get("type");
        final QueryInterpolatorFactory factory = tsdb.getRegistry().getPlugin(
            QueryInterpolatorFactory.class, 
            type_json == null || type_json.isNull() ? 
               null : type_json.asText());
        if (factory == null) {
          throw new IllegalArgumentException("Unable to find an "
              + "interpolator factory for: " + 
              (type_json == null || type_json.isNull() ? "Default" :
               type_json.asText()));
        }
        
        final QueryInterpolatorConfig interpolator_config = 
            factory.parseConfig(mapper, tsdb, config);
        builder.addInterpolatorConfig(interpolator_config);
      }
      
      n = node.get("serializeObserved");
      if (n != null && !n.isNull()) {
        builder.setSerializeObserved(n.asBoolean());
      }
      
      n = node.get("serializeThresholds");
      if (n != null && !n.isNull()) {
        builder.setSerializeThresholds(n.asBoolean());
      }
    }
  }
}

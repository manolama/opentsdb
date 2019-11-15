package net.opentsdb.query.egads.olympicscoring;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.egads.olympicscoring.OlympicScoringConfig.Builder;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.QueryInterpolatorFactory;

public class OlympicScoringFactory extends BaseQueryNodeFactory<OlympicScoringConfig, OlympicScoringNode> {
  public static final String TYPE = "OlympicScoring";

  private TSDB tsdb;
  
  @Override
  public OlympicScoringNode newNode(final QueryPipelineContext context,
                                    final OlympicScoringConfig config) {
    return new OlympicScoringNode(this, context, config);
  }
  
  @Override
  public OlympicScoringConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
      JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("baselineQuery");
    if (n != null && !n.isNull()) {
      builder.setBaselineQuery(SemanticQuery.parse(tsdb, n).build());
    }
    
    n = node.get("id");
    if (n != null) {
      builder.setId(n.asText());
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
    
    n = node.get("baselinePeriod");
    if (n != null && !n.isNull()) {
      builder.setBaselinePeriod(n.asText());
    }
    
    n = node.get("baselineNumPeriods");
    if (n != null && !n.isNull()) {
      builder.setBaselineNumPeriods(n.asInt());
    }
    
    n = node.get("baselineAggregator");
    if (n != null && !n.isNull()) {
      builder.setBaselineAggregator(n.asText());
    }
    
    n = node.get("excludeMax");
    if (n != null && !n.isNull()) {
      builder.setExcludeMax(n.asInt());
    }
    
    n = node.get("excludeMin");
    if (n != null && !n.isNull()) {
      builder.setExcludeMin(n.asInt());
    }
    
    n = node.get("upperThreshold");
    if (n != null && !n.isNull()) {
      builder.setUpperThreshold(n.asDouble());
    }
    
    n = node.get("upperIsScalar");
    if (n != null && !n.isNull()) {
      builder.setUpperIsScalar(n.asBoolean());
    }
    
    n = node.get("lowerThreshold");
    if (n != null && !n.isNull()) {
      builder.setLowerThreshold(n.asDouble());
    }
    
    n = node.get("lowerIsScalar");
    if (n != null && !n.isNull()) {
      builder.setLowerIsScalar(n.asBoolean());
    }
    
    
    return builder.build();
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }
}

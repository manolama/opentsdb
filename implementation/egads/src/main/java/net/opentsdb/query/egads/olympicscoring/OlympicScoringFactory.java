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
  public OlympicScoringConfig parseConfig(final ObjectMapper mapper, 
                                          final TSDB tsdb,
                                          final JsonNode node) {
    Builder builder = new Builder();
    Builder.parseConfig(mapper, tsdb, node, builder);
    
    JsonNode n = node.get("baselineQuery");
    if (n != null && !n.isNull()) {
      builder.setBaselineQuery(SemanticQuery.parse(tsdb, n).build());
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

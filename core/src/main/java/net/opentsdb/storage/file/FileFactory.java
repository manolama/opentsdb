package net.opentsdb.storage.file;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.MockDataStore;
import net.opentsdb.utils.JSON;

public class FileFactory extends BaseTSDBPlugin 
    implements TimeSeriesDataSourceFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileFactory.class);
  
  public static final String TYPE = "FileFactory";
  
  public JsonNode n1;
  public JsonNode n2;
  public JsonNode n3;
  public RollupConfig c;
  
  @Override
  public QueryNodeConfig parseConfig(ObjectMapper mapper, TSDB tsdb,
                                     JsonNode node) {
    return DefaultTimeSeriesDataSourceConfig.parseConfig(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(QueryPipelineContext context, QueryNodeConfig config,
      QueryPlanner planner) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context) {
    throw new UnsupportedOperationException("Doh");
  }

  @Override
  public QueryNode newNode(QueryPipelineContext context,
      QueryNodeConfig config) {
    return new FileNode(this, context, config);
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public boolean supportsQuery(TimeSeriesQuery query,
      TimeSeriesDataSourceConfig config) {
    return true;
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> operation) {
    return true;
  }

  @Override
  public Deferred<TimeSeriesStringId> resolveByteId(TimeSeriesByteId id,
      Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinKeys(List<String> join_keys,
      Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<byte[]>> encodeJoinMetrics(List<String> join_metrics,
      Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    //String dir = "file:///Users/clarsen/Documents/opentsdb/temp/";
    String dir = "file:///home/y/logs/";
    try {
      final String raw = Resources.toString(new URL(dir + "n1.json"), Const.UTF8_CHARSET);
      n1 = JSON.getMapper().readTree(raw).get("results").get(0);
      LOG.info("READ INPUT!");
    } catch (MalformedURLException e) {
      LOG.error("WTF??", e);
    } catch (IOException e) {
      LOG.error("WTF??", e);
    }
    
    try {
      final String raw = Resources.toString(new URL(dir + "n2.json"), Const.UTF8_CHARSET);
      n2 = JSON.getMapper().readTree(raw).get("results").get(0);
      LOG.info("READ INPUT!");
    } catch (MalformedURLException e) {
      LOG.error("WTF??", e);
    } catch (IOException e) {
      LOG.error("WTF??", e);
    }
    
    try {
      final String raw = Resources.toString(new URL(dir + "n3.json"), Const.UTF8_CHARSET);
      n3 = JSON.getMapper().readTree(raw).get("results").get(0);
      LOG.info("READ INPUT!");
    } catch (MalformedURLException e) {
      LOG.error("WTF??", e);
    } catch (IOException e) {
      LOG.error("WTF??", e);
    }
    
    c = DefaultRollupConfig.newBuilder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addInterval(RollupInterval.builder()
            .setInterval("1h")
            .setRowSpan("1d")
            .setTable("tsdb")
            .setPreAggregationTable("tsdb"))
        .build();
    
    
    return Deferred.fromResult(null);
  }
}

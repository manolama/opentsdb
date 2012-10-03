package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.HttpQuery;

/**
 * Handles formatting data as JSON using the native TSDB format
 */
public class TsdbJSON extends TSDFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbJSON.class);
  
  /** Typeref for Jackson to properly deserialize incoming data points */
  private final static TypeReference<ArrayList<TsdbJSONdp>> dpTypeRef = 
    new TypeReference<ArrayList<TsdbJSONdp>>() {};
  
  private static final AtomicLong puts_success = new AtomicLong();
  private static final AtomicLong puts_fail = new AtomicLong();
  
  /**
   * Default constructor
   * @param tsdb The TSDB object to work with
   */
  public TsdbJSON(final TSDB tsdb){
    super(tsdb);
  }
  
  /**
   * Parses the query string for options and then builds an array of TsdbJSONOutput
   * objects to return to the caller after serializing.
   * @param query The HTTPQuery to parse
   * @return Returns true
   */
  public boolean handleHTTPGet(final HttpQuery query){  
    List<TsdbJSONOutput> timeseries = new ArrayList<TsdbJSONOutput>();
    Boolean return_basic_meta = query.hasQueryStringParam("meta") ?
        query.parseBoolean(query.getQueryStringParam("meta")) : true;
    Boolean return_stats = query.hasQueryStringParam("stats") ?
        query.parseBoolean(query.getQueryStringParam("stats")) : true;
    Boolean return_uids = query.hasQueryStringParam("tsuids") ?
        query.parseBoolean(query.getQueryStringParam("tsuids")) : false;
        
    for (DataPoints dp : this.datapoints){
      TsdbJSONOutput ts = new TsdbJSONOutput();
      
      if (return_basic_meta){
        ts.metric = dp.metricName();
        if (dp.getTags() != null)
          ts.tags = dp.getTags();
        if (dp.getAggregatedTags() != null)
          ts.aggregated_tags = dp.getAggregatedTags();
      }
      
      ts.dps = new TreeMap<Long, Object>();
      for(int i=0; i<dp.size(); i++)
        ts.dps.put(dp.timestamp(i), 
            dp.isInteger(i) ? dp.longValue(i) : dp.doubleValue(i));
      
      if (return_uids)
        ts.tsuids = dp.getUID();
      
      if (return_stats){
        TsdbJSONStats stats = new TsdbJSONStats();
        stats.aggregated_datapoints = dp.size();
        if (dp.aggregatedSize() > 0)
          stats.raw_datapoints = dp.aggregatedSize();
        else
          stats.raw_datapoints = dp.size();
        stats.unique_tsids = dp.getUID().size();
        ts.stats = stats;
      }
      
      timeseries.add(ts);
    }
    
    JSON codec = new JSON(timeseries);
    query.sendReply(codec.getJsonBytes());    
    return true;
  }

  /**
   * Parses the POST data for datapoints to store.
   * @param query The HTTPQuery to parse
   * @return Returns true
   */
  @SuppressWarnings("unchecked")
  public boolean handleHTTPPut(final HttpQuery query){
    Boolean details = query.hasQueryStringParam("error_details") ?
        query.parseBoolean(query.getQueryStringParam("error_details")) : false;
    Boolean fault = query.hasQueryStringParam("fault_on_any") ?
        query.parseBoolean(query.getQueryStringParam("fault_on_any")) : false;
    Boolean return_json = query.hasQueryStringParam("json_return") ?
        query.parseBoolean(query.getQueryStringParam("json_return")) : true;  
        
    String json = query.getPostData();
    if (json == null || json.isEmpty()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Post data was empty");
      return false;
    }
    
    List<TsdbJSONdp> datapoints = new ArrayList<TsdbJSONdp>();
    JSON codec = new JSON(datapoints);
    if (!codec.parseObject(json, dpTypeRef)){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Error parsing JSON data");
      return false;
    }
    datapoints = (List<TsdbJSONdp>)codec.getObject();
    
    if (datapoints.size() < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse any data points");
      return false;
    }
    
    List<Map<String, Object>> errors = new ArrayList<Map<String, Object>>();
    Map<String, Object> err;
    long success = 0;
    long total = 0;
    
    // loop and store
    for (TsdbJSONdp dp : datapoints){
      total++;
      try{
        if (dp.metric.length() <= 0) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or empty metric name");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Empty metric name at [" + total + "]");
          continue;
        }
        if (dp.timestamp <= 0) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or invalid timestamp");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid timestamp at [" + total + "]");
          continue;
        }
        if (dp.value == null || dp.value.length() < 1) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or empty value");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid value at [" + total + "]");
          continue;
        }
        if (dp.tags == null || dp.tags.size() < 1) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing tags");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid tags at [" + total + "]");
          continue;
        }
        tsdb.addPoint(dp.metric, dp.timestamp, dp.value, dp.tags);
        success++;
      } catch (NumberFormatException nfe){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", nfe.getMessage());
          err.put("datapoint", dp);
          errors.add(err);
        }
        LOG.error(String.format("Unable to convert metric [%s] value [%s]: %s", 
            dp.metric, dp.value.toString(), nfe.getMessage()));
      } catch (IllegalArgumentException iae){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", iae.getMessage());
          err.put("datapoint", dp);
          errors.add(err);
        }
        LOG.error(String.format("Unable to convert metric [%s] value [%s]: %s", 
            dp.metric, dp.value.toString(), iae.getMessage()));
      }
    }
    
    this.puts_success.addAndGet(success);
    if (datapoints.size() != success)
      this.puts_fail.addAndGet(datapoints.size() - success);
    Map<String, Object> results = new HashMap<String, Object>();
    results.put("success", success);
    results.put("fail", datapoints.size() - success);
    if (details)
      results.put("errors", errors);
    
    if (!return_json){
      if (success < 1 || (fault && datapoints.size() != success))
        query.sendReply(HttpResponseStatus.BAD_REQUEST, "");
      else
        query.sendReply("");
    }else{
      codec = new JSON(results);
      if (success < 1 || (fault && datapoints.size() != success))
        query.sendReply(HttpResponseStatus.BAD_REQUEST, codec.getJsonString());
      else
        query.sendReply(codec.getJsonString());
    }
    
    return true;
  }
  
  public static void collectStats(final StatsCollector collector){
    collector.record("http.formatter.tsdbjson.put.success", puts_success.get());
    collector.record("http.formatter.tsdbjson.put.fail", puts_fail.get());
  }
//---------------- PRIVATES ------------------------------
  
  /**
   * A private helper class for serializing query results through Jackson
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @SuppressWarnings("unused")
  private static class TsdbJSONOutput {
    /** The name of the metric */    
    public String metric = null;
    /** A list of tagk/tagv values */
    public Map<String, String> tags = null;
    /** A list of all of the tags that were aggregated into the results */
    public List<String> aggregated_tags = null;
    /** A list of timestamp uids */
    public List<String> tsuids = null;
    /** The datapoints */
    public SortedMap<Long, Object> dps = null;
    /** Statistics from the query */
    public TsdbJSONStats stats = null;    
  }
  
  /**
   * Statistics about the query itself
   * TODO - move this somewhere else where it may be more useful
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @SuppressWarnings("unused")
  private static class TsdbJSONStats {
    /** The raw number of datapoints found in storage for the query */
    public long raw_datapoints;
    /** The number of datapoints actually returned */
    public long aggregated_datapoints;
    /** The amount of time it took to generate the results */
    public float time_taken;
    /** The number of unique timeseries IDs in the results */
    public long unique_tsids;
  }
  
  
  /**
   * A private class for deserializing incoming datapoints
   */
  private static class TsdbJSONdp {
    public String metric;
    public long timestamp;
    public String value;
    public Map<String, String> tags;
  }
}

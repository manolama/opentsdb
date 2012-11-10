package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.search.SearchQuery.SearchResults;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.StatsCollector.StatsDP;
import net.opentsdb.tsd.DataQuery;
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
  
  public String getEndpoint(){
    return "json";
  }
  
  /**
   * Parses the query string for options and then builds an array of TsdbJSONOutput
   * objects to return to the caller after serializing.
   * @param query The HTTPQuery to parse
   * @return Returns true
   */
  public boolean handleHTTPDataGet(final HttpQuery query){  
    List<TsdbJSONOutput> timeseries = new ArrayList<TsdbJSONOutput>();
    boolean return_basic_meta = query.hasQueryStringParam("meta") ?
        query.parseBoolean(query.getQueryStringParam("meta")) : true;
    boolean return_stats = query.hasQueryStringParam("stats") ?
        query.parseBoolean(query.getQueryStringParam("stats")) : true;
    boolean return_uids = query.hasQueryStringParam("wtsuids") ?
        query.parseBoolean(query.getQueryStringParam("wtsuids")) : false;
        
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
      ts.annotations = dp.getAnnotations();
      
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
  public boolean handleHTTPDataPut(final HttpQuery query){
    boolean details = query.hasQueryStringParam("error_details") ?
        query.parseBoolean(query.getQueryStringParam("error_details")) : false;
    boolean fault = query.hasQueryStringParam("fault_on_any") ?
        query.parseBoolean(query.getQueryStringParam("fault_on_any")) : false;
    boolean return_json = query.hasQueryStringParam("json_return") ?
        query.parseBoolean(query.getQueryStringParam("json_return")) : false;  
        
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
          LOG.warn("Empty metric name at [" + total + "]");
          continue;
        }
        if (dp.timestamp <= 0) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or invalid timestamp");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.warn("Invalid timestamp at [" + total + "]");
          continue;
        }
        if (dp.value == null || dp.value.length() < 1) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or empty value");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.warn("Invalid value at [" + total + "]");
          continue;
        }
        if (dp.tags == null || dp.tags.size() < 1) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing tags");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.warn("Invalid tags at [" + total + "]");
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
        LOG.warn(String.format("Unable to convert metric [%s] value [%s]: %s", 
            dp.metric, dp.value.toString(), nfe.getMessage()));
      } catch (IllegalArgumentException iae){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", iae.getMessage());
          err.put("datapoint", dp);
          errors.add(err);
        }
        LOG.warn(String.format("Unable to convert metric [%s] value [%s]: %s", 
            dp.metric, dp.value.toString(), iae.getMessage()));
      }
    }
    
    puts_success.addAndGet(success);
    if (datapoints.size() != success)
      puts_fail.addAndGet(datapoints.size() - success);

    if (!return_json){
      if (success < 1 || (fault && datapoints.size() != success))
        query.sendReply(HttpResponseStatus.BAD_REQUEST, "");
      else
        query.sendReply("");
    }else{
      Map<String, Object> results = new HashMap<String, Object>();
      results.put("success", success);
      results.put("fail", datapoints.size() - success);
      if (details)
        results.put("errors", errors);
      
      codec = new JSON(results);
      if (success < 1 || (fault && datapoints.size() != success))
        query.sendReply(HttpResponseStatus.BAD_REQUEST, codec.getJsonString());
      else
        query.sendReply(codec.getJsonString());
    }
    
    return true;
  }
  
  public boolean handleHTTPMetaGet(final HttpQuery query, final Object meta){
    JSON codec = new JSON(meta);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPStats(final HttpQuery query, ArrayList<StatsDP> stats){
    JSON codec = new JSON(stats);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPSearch(final HttpQuery query, SearchResults results){
    // build a response map and send away!
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", results.limit);
    response.put("page", results.page);
    response.put("total_uids", results.total_hits);
    response.put("total_pages", results.pages);
    response.put("time", results.time);
    if (results.short_meta != null)
      response.put("results", results.short_meta);
    else if (results.tsuids != null)
      response.put("results", results.tsuids);
    else if (results.ts_meta != null)
      response.put("results", results.ts_meta);
    else if (results.terms != null)
      response.put("results", results.terms);
    else if (results.annotations != null)
      response.put("results", results.annotations);
    else
      response.put("results", null);
    JSON codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPGroupby(final HttpQuery query, SearchResults results){
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", results.limit);
    response.put("page", results.page);
    response.put("total_pages", results.pages);
    response.put("time", results.time);
    if (results.terms != null){
      response.put("total_terms", results.terms.size());
      response.put("results", results.terms);
    }else if (results.groups != null){
      response.put("total_groups", results.total_groups);
      response.put("results", results.groups); 
      response.put("total_uids", results.total_hits);
    }else
      response.put("results", null); 
    JSON codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPSuggest(final HttpQuery query, final List<String> results){
    JSON codec = new JSON(results);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPVersion(final HttpQuery query, final HashMap<String, Object> version){
    JSON codec = new JSON(version);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPFormatters(final HttpQuery query, 
      final HashMap<String, ArrayList<String>> formatters){
    JSON codec = new JSON(formatters);
    query.sendReply(codec.getJsonBytes());
    return true;
  }

  public boolean handleHTTPAggregators(final HttpQuery query, final Set<String> aggregators){
    JSON codec = new JSON(aggregators);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public boolean handleHTTPAnnotation(final HttpQuery query, final List<Annotation> annotations){
    JSON codec = new JSON(annotations);
    query.sendReply(codec.getJsonBytes());
    return true;
  }
  
  public static void collectClassStats(final StatsCollector collector){
    collector.record("formatter.tsdbjson.put.success", puts_success.get());
    collector.record("formatter.tsdbjson.put.fail", puts_fail.get());
  }

  public String contentType(){
    return "application/json";
  }
  
  @Override
  public boolean validateQuery(final DataQuery query) {
    this.query = query;
    // TODO Auto-generated method stub
    return true;
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
    /** Annotations */
    public List<Annotation> annotations = null;
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
    public double time_taken;
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

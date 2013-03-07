// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.formatters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.tsd.DataQuery;
import net.opentsdb.tsd.HttpQuery;
import net.opentsdb.utils.JSON;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter for Collectd JSON data from the Write_HTTP plugin.
 * <p>
 * Example:
 * {"values":[4329984,1445888],"dstypes":["counter","counter"],"dsnames":["read"
 * ,"write"],"time":1328130386,"interval":10,"host":"hobbes-64bit","plugin":
 * "disk","plugin_instance":"sdb","type":"disk_octets","type_instance":""}
 */
//@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
//@JsonIgnoreProperties(ignoreUnknown = true)
public class CollectdJSON extends TSDFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(CollectdJSON.class);
  
  private static final AtomicLong puts_success = new AtomicLong();
  private static final AtomicLong puts_fail = new AtomicLong();
  private static final String metric_prefix = "sys.";
  
  /** Used for deserializing the Collectd JSON data */
  private static final TypeReference<ArrayList<CollectdJSONdps>> dpsTypeRef = 
    new TypeReference<ArrayList<CollectdJSONdps>>() {
  };

  /**
   * Default constructor
   * @param tsdb TSDB instance to use
   */
  public CollectdJSON(final TSDB tsdb){
    super(tsdb);
  }
  
  public String getEndpoint(){
    return "collectdjson";
  }
  
  /**
   * Parses the POST data for datapoints to store.
   * @param query The HTTPQuery to parse
   * @return Returns true if successful, false if there was an error
   */
  @SuppressWarnings("unchecked")
  public boolean handleHTTPDataPut(final HttpQuery query){
    Boolean details = query.hasQueryStringParam("error_details") ?
        query.parseBoolean(query.getQueryStringParam("error_details")) : false;
    Boolean fault = query.hasQueryStringParam("fault_on_any") ?
        query.parseBoolean(query.getQueryStringParam("fault_on_any")) : false;
    Boolean return_json = query.hasQueryStringParam("json_return") ?
        query.parseBoolean(query.getQueryStringParam("json_return")) : false;  
        
    String json = query.getPostData();
    if (json == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Post data was empty");
      return false;
    }
    
    // fix "nan" values
    json = json.replace("nan", "NaN");
    
    List<CollectdJSONdps> datapoints;
    try {
      datapoints = (List<CollectdJSONdps>)JSON.parseToObject(json, dpsTypeRef);
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return false;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return false;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return false;
    }
    
    if (datapoints.size() < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse any data points");
      return false;
    }
    
    List<Map<String, Object>> errors = new ArrayList<Map<String, Object>>();
    Map<String, Object> err;
    long success = 0;
    long total = 0;
    
    // loop and store
    for (CollectdJSONdps dp : datapoints){
      try{
        if (dp.values != null)
          total += dp.values.size();
        if (dp.values.size() < 1) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing values");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Empty values list at [" + total + "]");
          invalid_values.incrementAndGet();
          continue;
        }
        if (dp.time <= 0) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing or invalid timestamp");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid timestamp at [" + total + "]");
          invalid_values.incrementAndGet();
          continue;
        }
        if (dp.plugin.isEmpty()) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "Missing plugin name");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid plugin name at [" + total + "]");
          invalid_values.incrementAndGet();
          continue;
        }
        if (dp.dstypes.size() > 0 && (dp.values.size() != dp.dstypes.size() || 
            dp.dstypes.size() != dp.dsnames.size())) {
          if (details){
            err = new HashMap<String, Object>();
            err.put("error", "One of the array sizes is out of bounds");
            err.put("datapoint", dp);
            errors.add(err);
          }
          LOG.error("Invalid dstypes/value size at [" + total + "]");
          invalid_values.incrementAndGet();
          continue;
        }
        
        // we have to loop through the values and write a data point for each
        for (int i=0; i<dp.values.size(); i++){
          
          // build metric name
          StringBuilder metric = new StringBuilder(metric_prefix);
          metric.append(dp.plugin);
          
          // extend the metric name        
          if (!dp.type.isEmpty() && !dp.type.equals(dp.plugin)){
            metric.append(".");
            metric.append(dp.type);
          }
          
          if (!dp.plugin_instance.isEmpty()){
            metric.append(".");
            metric.append(dp.plugin_instance);
          }

          if (!dp.type_instance.isEmpty()){
            metric.append(".");
            metric.append(dp.type_instance);
          }
          
          if (dp.dsnames.size() > 0 && !dp.dsnames.get(i).equals(dp.plugin)
              && !dp.dsnames.get(i).equals("value")) {
            metric.append(".");
            metric.append(dp.dsnames.get(i));
          }

          // add host tag
          Map<String, String> tags = new HashMap<String, String>();
          tags.put("host", dp.host);
          
          tsdb.addPoint(metric.toString(), dp.time, dp.values.get(i), tags);
          success++; 
        }
      } catch (NumberFormatException nfe){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", nfe.getMessage());
          err.put("datapoints", dp);
          errors.add(err);
        }
        LOG.error(String.format("Unable to convert metric at [%d]: %s", 
            total, nfe.getMessage()));
        invalid_values.incrementAndGet();
      } catch (IllegalArgumentException iae){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", iae.getMessage());
          err.put("datapoints", dp);
          errors.add(err);
        }
        LOG.error(String.format("Unable to convert metric at [%d]: %s", 
            total, iae.getMessage()));
        invalid_values.incrementAndGet();
      } catch (TsdbStorageException tse){
        if (details){
          err = new HashMap<String, Object>();
          err.put("error", tse.getMessage());
          err.put("datapoints", dp);
          errors.add(err);
        }
        LOG.error(String.format("Unable to store metric at [%d]: %s", 
            total, tse.getMessage()));
        storage_errors.incrementAndGet();
      }
    }
    
    puts_success.addAndGet(success);
    if (total != success)
      puts_fail.addAndGet(total - success);

    if (!return_json){
      if (success < 1 || (fault && total != success))
        query.sendReply(HttpResponseStatus.BAD_REQUEST, "");
      else
        query.sendReply("");
    }else{
      Map<String, Object> results = new HashMap<String, Object>();
      results.put("success", success);
      results.put("fail", total - success);
      if (details)
        results.put("errors", errors);
      
      try {
        if (success < 1 || (fault && total != success))
          query.sendReply(HttpResponseStatus.BAD_REQUEST, JSON.serializeToString(results));
        else
          query.sendReply(JSON.serializeToString(results));
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return false;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return false;
      }
      
      return true;
    }
    return false;
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
  
  public static void collectClassStats(final StatsCollector collector){
    collector.record("formatter.collectdjson.put.success", puts_success.get());
    collector.record("formatter.collectdjson.put.fail", puts_fail.get());
  }
  
// ---------------- PRIVATES ------------------------------

  /**
   * Private class used for deserializing Collectd data from the write_http plugin
   */
  private static class CollectdJSONdps{
    // each array should have the same number of elements, if not, there was a
    // glitch
    /** Numeric values of the metric */
    public ArrayList<String> values = new ArrayList<String>();
    /** Type of data the value represents, e.g. counter, gauge, etc */
    public ArrayList<String> dstypes = new ArrayList<String>();
    /** Names of the different values */
    public ArrayList<String> dsnames = new ArrayList<String>();
    /** Unix epoch timestamp */
    public long time = 0;
    /** How often this metric is sent. Can use this for metadata in the future */
    public long interval = 0;
    /** Host from whence the data came */
    public String host = "";
    /** Name of the plugin that generated the metric */
    public String plugin = "";
    /** This is usually the device instance, such as "sdb" or "ef0" */
    public String plugin_instance = "";
    /** The type of data represented in the metric such as "disk_octets" */
    public String type = "";
    /** Alternative instance name */
    public String type_instance = "";
    
    /**
     * Method used by Jackson to capture unknown fields and log them. Without this
     * if the JSON format changes slightly (by adding a field) we wouldn't be able
     * to deserialize automatically. This method simply logs a notification.
     * @param key The JSON parameter
     * @param value The value of the JSON parameter
     */
    /* is used by Jackson */
    @JsonAnySetter
    private void handleUnknown(String key, Object value) {
      LOG.warn("Unrecognized JSON parameter [" + key + "] Value ["
          + value.toString() + "]");
    }
  }
}

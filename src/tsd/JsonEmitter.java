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
package net.opentsdb.tsd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoint;

/**
 * Converts the data points returned from an HBase query into
 * Metric objects, then serializes them into a JSON string
 */
class JsonEmitter extends DataEmitter {
  //overload the log factory
  private static final Logger LOG = LoggerFactory.getLogger(JsonEmitter.class);
  
  /** Stores the metric data converted from the data points */
  ArrayList<Metric> metrics = new ArrayList<Metric>();

  /** JSONP function if the user requests it */
  String jsonp = "";

  /**
   * Constructor to initialize our emitter
   * @param start_time Start time of the query
   * @param end_time End time of the query
   */
  public JsonEmitter(long start_time, long end_time,
      final Map<String, List<String>> qs, final int queryHash) {
    super(start_time, end_time, qs, queryHash);
    
    // see if we have a function for the jsonp
    if (this.query_string.containsKey("json")){
      List<String> temp = this.query_string.get("json");
      jsonp = temp.size() < 1 ? "" : temp.get(temp.size() - 1);
    }
  }

  /**
   * Loads the data points into an array of Metric objects that will then be
   * serialized into JSON
   */
  public final boolean processData() {
    if (datapoints.size() < 1) {
      error = "No data to process";
      LOG.error(error);
      return false;
    }

    try{
      // copy data points into the Metrics arrays
      final int nseries = datapoints.size();
      for (int i = 0; i < nseries; i++) {
        Metric m = new Metric();
        m.data = new ArrayList<MetricDP>();
  
        // copy meta info
        m.setMetric(datapoints.get(i).metricName());
        m.setTags(datapoints.get(i).getTags());
  
        // load data points into the metric
        for (final DataPoint d : datapoints.get(i)) {
  
          // temp storage for figuring out what kind of value we have
          Object val = null;
          if (d.isInteger()) {
            val = d.longValue();
          } else {
            final double value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity found in"
                  + " datapoints #" + i + ": " + value + " d=" + d);
            }
            val = value;
          }
          // add the data point
          m.data.add(new MetricDP(d.timestamp(), val));
        }
  
        // add the metric to the array
        this.metrics.add(m);
      }
    } catch(IllegalStateException e){
      error = e.getMessage();
      LOG.error(error);
      return false;
    }

    // we got stuff!
    processed = true;
    return true;
  }

  /**
   * Gets the results to be cached and/or returned to the user
   * @return Returns null if the data was not processed, otherwise
   * it returns an object to be stored in the cache
   */
  public final HttpCacheEntry getCacheData(){ 
    if (!processed) {
      LOG.error("Object was not processed");
      return null;
    }
    
    // now we have all the metrics stored, serialize and return
    JSON_HTTP json = new JSON_HTTP(this.metrics);
    String response = jsonp.isEmpty() ? json.getJsonString() : json
        .getJsonPString(jsonp);
    
    return new HttpCacheEntry(
        query_hash,
        (response.isEmpty() ? json.getError().getBytes() : response.getBytes()),
        basepath + ".json",
        false,
        this.computeExpire()
    );
  }
}

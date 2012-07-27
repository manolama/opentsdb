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
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Represents a single Metric object with associated timestamp, name, value and
 * tags
 * <p>
 * This class is for automatic de/serialization of JSON data with the Jackson
 * library. It also provides a shortcut for creating a useful error message that
 * can be sent back to API clients
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Metric {
  /** The name of the metric **/
  private String metric = "";
  
  private String uid = "";
  
  /** The unix epoch timestamp the metric was generated **/
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  private long timestamp = 0;
  /** The actual value of the metric **/
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  private String value = "";
  /** A map of tag names and values for the metric **/
  private Map<String, String> tags = null;
  /** Array of data points for emitters */
  public ArrayList<MetricDP> data = null;
  
  /**
   * Builds an error map consisting of an error message and the actual metric
   * object itself. This can be sent to a serializer like Jackson to be
   * converted to JSON for API responses Note: If the user supplies an empty
   * string for the message, it will store "Unknown error"
   * @param msg A descriptive error message of why this metric generated an
   *          error
   * @return A map with an "error" and a "metric" object
   */
  public Map<String, Object> BuildError(final String msg) {
    Map<String, Object> err = new HashMap<String, Object>();
    err.put("error", (msg.isEmpty() ? "Unknown error" : msg));
    err.put("metric", this);
    return err;
  }

  // **** GETTERS AND SETTERS ****
  /** @return The name of the metric */
  public String getMetric() {
    return metric;
  }

  public String getUID(){
    return this.uid;
  }
  
  /** @return The unix epoch timestamp of the metric */
  public long getTimestamp() {
    return timestamp;
  }

  /** @return The numeric value of the metric */
  public String getValue() {
    return value;
  }

  /** @return A map of tags and values representing the metric */
  public Map<String, String> getTags() {
    return tags;
  }

  /** @param m The name of the metric to store */
  public void setMetric(String m) {
    metric = m;
  }

  public void setUID(String u) {
    this.uid = u;
  }
  
  /** @param t The unix epoch timestamp of the metric */
  public void setTimestamp(long t) {
    timestamp = t;
  }

  /** @param v The value for the metric */
  public void setValue(String v) {
    value = v;
  }

  /** @param t A map of tags and values for the metric */
  public void setTags(Map<String, String> t) {
    tags = t;
  }
}

/**
 * Represents a single data point in the time series for a particular
 * metric. The Metric class will have an array of these
 */
class MetricDP{
  public long t;
  public Object v;
  
  /**
   * Constructor for the data point
   * @param timestamp Unix epoch timestamp 
   * @param value Value of the metric
   */
  public MetricDP(long timestamp, Object value) {
    t = timestamp;
    v = value;
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Either a TSUID or a metric query for the last data point. For metrics we scan
 * in case the tags are a filter and for TSUIDs we do direct gets.
 * @since 2.1
 */
public class LastPointSubQuery {

  /** The metric to search for */
  private String metric;

  /** The list of tags to search for. May be empty */
  private HashMap<String, String> tags;

  /** A list of TSUIDs to get the last value for */
  private List<String> tsuids;
  
  /**
   * Default constructor necessary for de/serialization
   */
  public LastPointSubQuery() {
    
  }
  
  /**
   * Parses a standard metric query, replacing the key "m" with "timeseries".
   * It should be in the format "<metric>{<tagk>=<tagv>..}"
   * @param query the query string to parse out.
   * @return A sub query object
   */
  public static LastPointSubQuery parseTimeSeriesQuery(final String query) {
    final LastPointSubQuery sub_query = new LastPointSubQuery();
    sub_query.tags = new HashMap<String, String>();
    sub_query.metric = Tags.parseWithMetric(query, sub_query.tags);
    return sub_query;
  }
  
  /**
   * Parses a comma separate TSUID query where the TSUIDs are hex encoded.
   * @param query the query string to parse out.
   * @return A sub query object
   */
  public static LastPointSubQuery parseTSUIDQuery(final String query) {
    final LastPointSubQuery sub_query = new LastPointSubQuery();
    final String[] tsuids = query.split(",");
    sub_query.tsuids = new ArrayList<String>(tsuids.length);
    for (String tsuid : tsuids) {
      sub_query.tsuids.add(tsuid);
    }
    return sub_query;
  }
  
  /** @return The name of the metric to search for */
  public String getMetric() {
    return metric;
  }
  
  /** @return A map of tag names and values */
  public Map<String, String> getTags() {
    return tags;
  }
  
  /** @return A list of TSUIDs to get the last point for */
  public List<String> getTSUIDs() {
    return tsuids;
  }
  
  /** @param metric The metric to search for */
  public void setMetric(final String metric) {
    this.metric = metric;
  }
  
  /** @param tags A map of tag name/value pairs */
  public void setTags(final Map<String, String> tags) {
    this.tags = (HashMap<String, String>) tags;
  }
  
  /** @param tsuids A list of TSUIDs to get data for */
  public void setTSUIDs(final List<String> tsuids) {
    this.tsuids = tsuids;
  }
}

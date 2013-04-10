// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
import java.util.NoSuchElementException;

import net.opentsdb.utils.DateTime;

public class TSSubQuery {
  private String aggregator;
  
  private String metric;
  
  private ArrayList<String> tsuids;
  
  private HashMap<String, String> tags;
  
  private String downsample;
  
  private boolean rate;
  
  // local vars set after parsing
  private Aggregator agg;
  private Aggregator downsampler;
  private long downsample_interval;
  
  /**
   * Runs through query parameters to make sure it's a valid request.
   * This includes parsing the aggreagor, downsampling info, metrics, tags or
   * timeseries. 
   * @throws IllegalArgumentException if something is wrong with the query
   */
  public final void validateAndSetQuery() {
    if (this.aggregator == null || this.aggregator.isEmpty()) {
      throw new IllegalArgumentException("Missing the aggregation function");
    }
    try {
      this.agg = Aggregators.get(this.aggregator);
    } catch (NoSuchElementException nse) {
      throw new IllegalArgumentException(
          "No such aggregation function: " + this.aggregator);
    }
    
    // we must have at least one TSUID OR a metric
    if ((this.tsuids == null || this.tsuids.size() < 1) && 
        (this.metric == null || this.metric.isEmpty())) {
      throw new IllegalArgumentException(
          "Missing the metric or tsuids, provide at least one");
    }
    
    // parse the downsampler if we have one
    if (this.downsample != null && !this.downsample.isEmpty()) {
      final int dash = this.downsample.indexOf('-', 1); // 1st char can't be
                                                        // `-'.
      if (dash < 0) {
        throw new IllegalArgumentException("Invalid downsampling specifier '" 
            + this.downsample + "' in [" + this.downsample + "]");
      }
      try {
        downsampler = Aggregators.get(this.downsample.substring(dash + 1));
      } catch (NoSuchElementException e) {
        throw new IllegalArgumentException("No such downsampling function: "
            + this.downsample.substring(dash + 1));
      }
      downsample_interval = DateTime.parseDuration(
          this.downsample.substring(0, dash));
    }
  }
  
  /**
   * Returns a list of queries to execute
   * @return
   */
  public final List<Query> queries() {
    return null;
  }
}

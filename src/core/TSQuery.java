// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that defines a timeseries data query. This can be used for generating
 * a query to be passed on to a TSD. This defines the data to be fetched and
 * what transformations should be performed on that data after it's fetched.
 * 
 * Each TSQuery will have one or more MetricQueries that represent a single 
 * metric or timeseries. The familiar query line syntax maps an "m=" to a 
 * metric query. Multiple "m=" parameters result in multiple metric queries.
 * @since 2.0
 */
public final class TSQuery {
  private static final Logger LOG = LoggerFactory.getLogger(TSQuery.class);
  
  /** A user supplied absolute or relative starting time for the data */
  private String start;
  
  /** A user supplied absolute or relative ending time for the data */
  private String end;
  
  /** A list of one or more MetricQuery objects */
  private ArrayList<MetricQuery> queries;
  
  /** Whether or not to include padding in the results */
  private boolean with_padding = false;
  
  /** Whether or not to search for global annotations in the same time frame */
  public boolean with_global_annotations;
  
  /** An optional TopN query */
  public TopNQuery topn;
  
  /**
   * Represents a query for a single metric or timeseries.
   * A MetricQuery can have either a metric and tags OR timeseries supplied. 
   * If a timeseries is supplied, then the metric and tags are ignored.
   * @since 2.0
   */
  public static final class MetricQuery {   
    /** Name of the metric. May be null or empty. */
    public String metric;
    
    /** Map of tagk/tagv pairs from the user. May be empty if the user wants all
     * of the metrics. */
    public HashMap<String, String> tags;
    
    /** A list of timeseries IDs to aggregate. May be null or empty. */
    public ArrayList<String> tsuids;
    
    /** Name of the aggregator to use for this query */
    public String aggregator;
    
    /** ? I forgot, need to retrace */
    public String type;
    
    /** Optional downsampling notation */
    public String downsample;
    
    /** Whether or not to return local annotations */
    public boolean with_local_annotations;
  }
  
  /**
   * Represents a request for a TopN query where a request for a number of
   * separate groups or individual timeseries is sorted and only the top "N" 
   * number of results are returned.
   */
  public static final class TopNQuery {
    /** The aggregator to use */
    public String aggregator;
    
    /** The order, either "asc" or "desc" */
    public String order;
    
    /** The N number of results to return */
    public int limit;
  }
}

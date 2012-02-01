// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the default class that handles taking in data points read from HBase
 * and lets sub-classes define how to format and output that data to the user
 */
class DataEmitter {
  protected static final Logger LOG = LoggerFactory.getLogger(DataEmitter.class);

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  protected int start_time;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  protected int end_time;

  /** All the DataPoints we want to work with */
  protected ArrayList<DataPoints> datapoints =
    new ArrayList<DataPoints>();
  
  /** Flag to determine if we have formatted/processed the datapoints yet */
  protected boolean processed = false;
  
  /** Query parameters from the HTTP query */
  protected final Map<String, List<String>> query_string;
  
  /** Base path for the cache directory and query hash to use in storing files */
  protected String basepath = "";
  
  /** Hash code for the original query */
  protected int query_hash = 0;
  
  /** Error message implementers can check */
  protected String error = "";
  
  /**
   * Default constructor that verifies we have valid start and end times
   * @param start_time Timestamp of the start time of the result.
   * @param end_time Timestamp of the end time of the graph.
   * @param queryString Map of query parameters
   * @param queryHash Hash code of the original query
   * @throws IllegalArgumentException if either result is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   */
  public DataEmitter(final long start_time, final long end_time, 
      final Map<String, List<String>> queryString, final int queryHash) {
    if ((start_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid start time: " + start_time);
    } else if ((end_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid end time: " + end_time);
    } else if (start_time >= end_time) {
      throw new IllegalArgumentException("start time (" + start_time
        + ") is greater than or equal to end time: " + end_time);
    }
    this.start_time = (int) start_time;
    this.end_time = (int) end_time;
    this.query_string = queryString;
    this.query_hash = queryHash;
  }
  
  /**
   * Adds some data points to this emitter.
   * @param datapoints The data points to store.
   * @param options The options to apply to this specific series.
   */
  public final void add(final DataPoints datapoints) {
    // Technically, we could check the number of data points in the
    // datapoints argument in order to do something when there are none, but
    // this is potentially expensive with a SpanGroup since it requires
    // iterating through the entire SpanGroup.  We'll check this later
    // when we're trying to use the data, in order to avoid multiple passes
    // through the entire data.
    this.datapoints.add(datapoints);
  }
  
  /**
   * Returns a view on the datapoints in this emitter.
   * Do not attempt to modify the return value.
   */
  public final Iterable<DataPoints> getDataPoints() {
    return datapoints;
  }

  /** 
   * Overloaded by subclasses to actually process and format the 
   * data as needed
   * @return True if processing was successful, false if there was an error
   */
  public boolean processData(){
    LOG.warn("Not implemented");
    return false;
  }

  /**
   * Return the path to a file we need to send to the query
   * @return Path of a file or an empty string if not applicable
   */
  public String getDataFile(){
    LOG.warn("Not implemented");
    return "";
  }
  
  /**
   * Sets the base path
   * @param bp Path to use
   */
  public final void setBasepath(final String bp){
    basepath = bp;
    LOG.debug("Basepath: " + basepath);
  }
  
  /**
   * Getter to work with an error message
   * @return Error message
   */
  public final String getError(){
    return error;
  }

  /**
   * Stub to get a cache entry to store and/or return to the user
   * @return
   */
  public HttpCacheEntry getCacheData(){
    LOG.warn("Not implemented");
    return null;
  }

  /**
   * Default method to compute the amount of time before the
   * emitted data should expire from the local and client's cache
   * @return 0 if the data should not be cached, or a positive integer
   * reflecting how many seconds this data should be cached for
   */
  protected Long computeExpire(){
    // If the end time is in the future (1), make the graph uncacheable.
    // Otherwise, if the end time is far enough in the past (2) such that
    // no TSD can still be writing to rows for that time span and it's not
    // specified in a relative fashion (3) (e.g. "1d-ago"), make the graph
    // cacheable for a day since it's very unlikely that any data will change
    // for this time span.
    // Otherwise (4), allow the client to cache the graph for ~0.1% of the
    // time span covered by the request e.g., for 1h of data, it's OK to
    // serve something 3s stale, for 1d of data, 84s stale.
    final long now = System.currentTimeMillis() / 1000L;
    if (end_time > now) {                            // (1)
      return 0L;
    } else if (end_time < now - Const.MAX_TIMESPAN) { // (2)(3)
      return 86400L;
    } else {                                         // (4)
      return (long) ((end_time - start_time) >> 10);
    }
  }
}

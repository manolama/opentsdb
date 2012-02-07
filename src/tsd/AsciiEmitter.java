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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;

/**
 * Converts the data points returned from an HBase query into
 * an ASCII string of metrics
 */
public class AsciiEmitter extends DataEmitter {
  // overload the log factory
  private static final Logger LOG = LoggerFactory.getLogger(AsciiEmitter.class);
  
  /** Stores the ascii string generated */
  private String ascii = "";
  
  /**
   * Constructor to initialize our emitter
   * @param start_time Start time of the query
   * @param end_time End time of the query
   */
  public AsciiEmitter(long start_time, long end_time,
      final Map<String, List<String>> qs, final int queryHash) {
    super(start_time, end_time, qs, queryHash);
  }

  /**
   * Loops through the data points, building a big ole string
   */
  public boolean processData(){
    if (datapoints.size() < 1) {
      error = "No data to process";
      LOG.error(error);
      return false;
    }
    
    try{
      final StringBuilder tagbuf = new StringBuilder();
      for (final DataPoints dp : datapoints) {
        final String metric = dp.metricName();
        tagbuf.setLength(0);
        for (final Map.Entry<String, String> tag : dp.getTags().entrySet()) {
          tagbuf.append(' ').append(tag.getKey())
            .append('=').append(tag.getValue());
        }
        for (final DataPoint d : dp) {
          ascii += metric + " " + d.timestamp() + " ";
          if (d.isInteger()) {
            ascii += d.longValue();
          } else {
            final double value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity:" + value
                + " d=" + d);
            }
            ascii += value;
          }
          ascii += tagbuf + "\n";
        }
      }
    } catch(IllegalStateException e){
      error = e.getMessage();
      LOG.error(error);
      return false;
    }
    
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
      LOG.error("Data was not processed");
      return null;
    }
    
    return new HttpCacheEntry(
        query_hash,
        ascii.getBytes(),
        basepath + ".json",
        false,
        this.computeExpire()
    );
  }
}

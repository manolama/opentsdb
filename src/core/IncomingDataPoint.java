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

import java.util.HashMap;

/**
 * Bridging class that stores a normalized data point parsed from the "put" 
 * RPC methods and gets it ready for storage. Also has some helper methods that
 * were formerly in the Tags class for parsing values.
 * 
 * The data point value is a string in order to accept a wide range of values
 * including floating point and scientific. Before storage, the value will
 * be parsed to the appropriate type
 */
public final class IncomingDataPoint {

  private String metric;
  
  private long timestamp;
  
  private String value;
  
  private HashMap<String, String> tags;
  
  /**
   * Empty constructor necessary for some de/serializers
   */
  public IncomingDataPoint() {
    
  }
  
  public IncomingDataPoint(final String metric,
                           final long timestamp,
                           final String value) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
  }
  
  public IncomingDataPoint(final String metric,
      final long timestamp,
      final String value,
      final HashMap<String, String> tags) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
    this.tags = tags;
  }
  
  
}

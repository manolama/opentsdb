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
package net.opentsdb.tsd;

import java.util.Map;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * Real Time publisher plugin interface that is used to send data points to 
 * external systems as soon as they are processed for storage by OpenTSDB.
 */
public abstract class RTPublisher {

  public abstract void initialize(final TSDB tsdb);
  
  public abstract Deferred<Object> shutdown();
  
  public abstract String version();
  
  public abstract void collectStats(final StatsCollector collector);
  
  public final Deferred<Object> sink(final String metric, 
      final long timestamp, final byte[] value, final Map<String, String> tags, 
      final byte[] tsuid, final short flags) {
    
    return publish(metric, timestamp, 0L, tags, tsuid);
  }
  
  public abstract Deferred<Object> publish(final String metric, 
      final long timestamp, final long value, final Map<String, String> tags, 
      final byte[] tsuid);
  
  public abstract Deferred<Object> publish(final String metric, 
      final long timestamp, final float value, final Map<String, String> tags, 
      final byte[] tsuid);
}

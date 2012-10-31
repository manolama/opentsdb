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

import net.opentsdb.cache.Cache;
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.CollectdJSON;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.StatsCollector.StatsDP;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.stumbleupon.async.Deferred;

/**
 * Returns statistics about the current TSD operation
 */
final class StatsRPC implements TelnetRpc, HttpRpc {
  /**
   * Returns the statistics as ASCII text to the telnet caller
   * @param tsdb The tsd to fech information from
   * @param chan Telnet channel to respond to
   * @param cmd Commandline text
   * @return Deferred<Object> async object for the Telnet client to parse
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd, final TSDFormatter formatter) {
    final ArrayList<StatsDP> datapoints = doCollectStats(tsdb);
    formatter.handleTelnetStats(cmd, chan, datapoints);
    return Deferred.fromResult(null);
  }

  /**
   * Returns the statistics as an ASCII string or JSON. Does not perform any
   * caching
   * @param tsdb TSD to fetch data from
   * @param query HTTP query to respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final ArrayList<StatsDP> datapoints = doCollectStats(tsdb);

    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    formatter.handleHTTPStats(query, datapoints);
  }

  /**
   * Sets up the collector with a host tag and then peeks into all of the proper
   * classes to pull data
   * @param tsdb TSD to collect data from
   * @param collector Collector to store data in
   */
  private ArrayList<StatsDP> doCollectStats(final TSDB tsdb) {
    final ArrayList<StatsDP> datapoints = new ArrayList<StatsDP>();
    final StatsCollector collector = new StatsCollector("tsd") {
      @Override
      public final void emit(final StatsDP dp) {
        datapoints.add(dp);
      }
    };
    
    collector.addHostTag();
    ConnectionManager.collectStats(collector);
    Cache.collectStats(collector);
    RpcHandler.collectStats(collector);
    tsdb.collectStats(collector);
    //PipelineFactory.collectStats(collector);
    TSDFormatter.collectStats(collector, tsdb);
    return datapoints;
  }
}

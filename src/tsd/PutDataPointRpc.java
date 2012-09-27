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

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.formatters.Ascii;
import net.opentsdb.formatters.CollectdJSON;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueName;

/** Implements the "put" telnet-style command. */
final class PutDataPointRpc implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory
      .getLogger(PutDataPointRpc.class);
  private static final AtomicLong requests = new AtomicLong();
  private static final AtomicLong hbase_errors = new AtomicLong();
  private static final AtomicLong invalid_values = new AtomicLong();
  private static final AtomicLong illegal_arguments = new AtomicLong();
  private static final AtomicLong unknown_metrics = new AtomicLong();

  /**
   * Handles the HTTP PUT command
   * <p>
   * Metrics should be sent in a JSON format via POST. The metrics format is:
   * {"metric":"metric_name","timestamp":unix_epoch_time,"value":value",
   * "tags":{"tag1":"tag_value1","tagN":"tag_valueN"}} You can combine multiple
   * metrics in a single JSON array such as [{metric1},{metric2}]
   * <p>
   * This method will respond with a JSON string that lists the number of
   * successfully parsed metrics and the number of failed metrics, along with a
   * list of which metrics failed and why. If the JSON was improperly formatted
   * or there was another error, a JSON-RPC style error will be returned.
   * @param tsdb Master TSDB class object
   * @param query The query from Netty
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    LOG.trace(String.format("HTTP Thread ID [%d]", Thread.currentThread().getId()));
    String endpoint = query.getEndpoint();
    final TSDFormatter formatter;
    if (endpoint != null){
      if (endpoint.compareTo("ascii") == 0){
        formatter = new Ascii(tsdb);
      }else if (endpoint.compareTo("collectdjson") == 0){
        formatter = new CollectdJSON(tsdb);
      }else{
        formatter = new TsdbJSON(tsdb);
      }
    }else
      formatter = new TsdbJSON(tsdb);
    
    formatter.handleHTTPPut(query);
    return;
  }

  /**
   * Handles the Telnet PUT command
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd) {
    requests.incrementAndGet();
    String errmsg = null;
    try {
      final class PutErrback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          if (chan.isConnected()) {
            chan.write("put: HBase error: " + arg.getMessage() + '\n');
          }
          hbase_errors.incrementAndGet();
          return arg;
        }

        public String toString() {
          return "report error to channel";
        }
      }
      return importDataPoint(tsdb, cmd).addErrback(new PutErrback());
    } catch (NumberFormatException x) {
      errmsg = "put: invalid value: " + x.getMessage() + '\n';
      invalid_values.incrementAndGet();
    } catch (IllegalArgumentException x) {
      errmsg = "put: illegal argument: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (NoSuchUniqueName x) {
      errmsg = "put: unknown metric: " + x.getMessage() + '\n';
      unknown_metrics.incrementAndGet();
    }
    if (errmsg != null && chan.isConnected()) {
      chan.write(errmsg);
    }
    return Deferred.fromResult(null);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("rpc.received", requests, "type=put");
    collector.record("rpc.errors", hbase_errors, "type=hbase_errors");
    collector.record("rpc.errors", invalid_values, "type=invalid_values");
    collector.record("rpc.errors", illegal_arguments, "type=illegal_arguments");
    collector.record("rpc.errors", unknown_metrics, "type=unknown_metrics");
  }

  /**
   * Imports a single data point.
   * @param tsdb The TSDB to import the data point into.
   * @param words The words describing the data point to import, in the
   *          following format: {@code [metric, timestamp, value, ..tags..]}
   * @return A deferred object that indicates the completion of the request.
   * @throws NumberFormatException if the timestamp or value is invalid.
   * @throws IllegalArgumentException if any other argument is invalid.
   * @throws NoSuchUniqueName if the metric isn't registered.
   */
  private Deferred<Object> importDataPoint(final TSDB tsdb, final String[] words) {
    words[0] = null; // Ditch the "put".
    if (words.length < 5) { // Need at least: metric timestamp value tag
      // ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
          + " (need least 4, got " + (words.length - 1) + ')');
    }
    final String metric = words[1];
    if (metric.length() <= 0) {
      throw new IllegalArgumentException("empty metric name");
    }
    final long timestamp = Tags.parseLong(words[2]);
    if (timestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    final String value = words[3];
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    if (value.indexOf('.') < 0) { // integer value
      return tsdb.addPoint(metric, timestamp, Tags.parseLong(value), tags);
    } else { // floating point value
      return tsdb.addPoint(metric, timestamp, Float.parseFloat(value), tags);
    }
  }
}

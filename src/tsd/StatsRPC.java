package net.opentsdb.tsd;

import java.util.ArrayList;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Returns statistics about the current TSD operation
 */
final class StatsRPC implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(StatsRPC.class);

  /**
   * Returns the statistics as ASCII text to the telnet caller
   * @param tsdb The tsd to fech information from
   * @param chan Telnet channel to respond to
   * @param cmd Commandline text
   * @return Deferred<Object> async object for the Telnet client to parse
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd) {
    final StringBuilder buf = new StringBuilder(1024);
    final StatsCollector collector = new StatsCollector("tsd") {
      @Override
      public final void emit(final String line) {
        buf.append(line);
      }
    };
    doCollectStats(tsdb, collector);
    chan.write(buf.toString());
    return Deferred.fromResult(null);
  }

  /**
   * Returns the statistics as an ASCII string or JSON. Does not perform any
   * caching
   * @param tsdb TSD to fetch data from
   * @param query HTTP query to respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean json = JsonHelper.getJsonRequested(query);
    final StringBuilder buf = json ? null : new StringBuilder(2048);
    final ArrayList<String> stats = json ? new ArrayList<String>(64) : null;
    final StatsCollector collector = new StatsCollector("tsd") {
      @Override
      public final void emit(final String line) {
        if (json) {
          stats.add(line.substring(0, line.length() - 1)); // strip the '\n'
        } else {
          buf.append(line);
        }
      }
    };
    doCollectStats(tsdb, collector);
    // handle JSON
    if (json) {
      final String jsonp = JsonHelper.getJsonPFunction(query);
      final JsonHelper response = new JsonHelper(stats);
      query.sendReply(jsonp.isEmpty() ? response.getJsonString() : response
          .getJsonPString(jsonp));
    } else {
      query.sendReply(buf);
    }
  }

  /**
   * Sets up the collector with a host tag and then peeks into all of the proper
   * classes to pull data
   * @param tsdb TSD to collect data from
   * @param collector Collector to store data in
   */
  private void doCollectStats(final TSDB tsdb, final StatsCollector collector) {
    collector.addHostTag();
    ConnectionManager.collectStats(collector);
    HttpCache.collectStats(collector);
    RpcHandler.collectStats(collector);
    tsdb.collectStats(collector);
  }
}

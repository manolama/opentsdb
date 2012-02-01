package net.opentsdb.tsd;

import java.util.HashMap;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Responds to the querent with the version of the local TSD
 */
final class VersionRPC implements TelnetRpc, HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(SuggestRPC.class);
  
  /**
   * Fetches the version of this TSD as a string
   * @param tsdb The TSDB to fetch data from
   * @param chan Telnet channel to respond to
   * @param cmd Telnet command received
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd) {
    if (chan.isConnected()) {
      chan.write(BuildData.revisionString() + '\n' + BuildData.buildString()
          + '\n');
    }
    return Deferred.fromResult(null);
  }

  /**
   * Returns the version information as a simple string (Default) or
   * in a JSON format, caching either response for a full day
   * @param tsdb The TSDB to fetch data from
   * @param query HTTP Query to send the response to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && HttpCache.readCache(query_hash, query)) {
      return;
    }

    // Send in JSON if requested
    if (JsonHelper.getJsonRequested(query)) {
      final String jsonp = JsonHelper.getJsonPFunction(query);
      HashMap<String, Object> version = new HashMap<String, Object>();
      version.put("short_revision", BuildData.short_revision);
      version.put("full_revision", BuildData.full_revision);
      version.put("timestamp", BuildData.timestamp);
      version.put("repo_status", BuildData.repo_status);
      version.put("user", BuildData.user);
      version.put("host", BuildData.host);
      version.put("repo", BuildData.repo);
      final JsonHelper response = new JsonHelper(version);

      // build our cache object, store and reply
      HttpCacheEntry entry = new HttpCacheEntry(query_hash,
          jsonp.isEmpty() ? response.getJsonString().getBytes() : response
              .getJsonPString(jsonp).getBytes(), "", /*
                                                      * don't bother persisting
                                                      * these
                                                      */
          false, 86400);
      if (!nocache && !HttpCache.storeCache(entry)) {
        LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
      }
      query.sendReply(entry.getData());
    } else {
      StringBuilder buf;
      final String revision = BuildData.revisionString();
      final String build = BuildData.buildString();
      buf = new StringBuilder(2 // For the \n's
          + revision.length() + build.length());
      buf.append(revision).append('\n').append(build).append('\n');

      // build our cache object, store and reply
      HttpCacheEntry entry = new HttpCacheEntry(query_hash, buf.toString()
          .getBytes(), "", /* don't bother persisting these */
      false, 86400);
      if (!nocache && !HttpCache.storeCache(entry)) {
        LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
      }
      query.sendReply(entry.getData());
    }
  }
}

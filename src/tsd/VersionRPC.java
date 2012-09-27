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

import java.util.HashMap;

import net.opentsdb.BuildData;
import net.opentsdb.cache.CacheEntry;
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
    if (!nocache && query.getCacheAndReturn(query_hash)) {
      return;
    }

    // Send in JSON if requested
    if (JSON_HTTP.getJsonRequested(query)) {
      final String jsonp = JSON_HTTP.getJsonPFunction(query);
      HashMap<String, Object> version = new HashMap<String, Object>();
      version.put("short_revision", BuildData.short_revision);
      version.put("full_revision", BuildData.full_revision);
      version.put("timestamp", BuildData.timestamp);
      version.put("repo_status", BuildData.repo_status);
      version.put("user", BuildData.user);
      version.put("host", BuildData.host);
      version.put("repo", BuildData.repo);
      final JSON_HTTP response = new JSON_HTTP(version);

      // build our cache object, store and reply
      CacheEntry entry = new CacheEntry(query_hash,
          jsonp.isEmpty() ? response.getJsonString().getBytes() : response
              .getJsonPString(jsonp).getBytes(), 86400);
      if (!nocache && !query.putCache(entry)) {
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
      CacheEntry entry = new CacheEntry(query_hash, buf.toString()
          .getBytes(), 86400);
      if (!nocache && !query.putCache(entry)) {
        LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
      }
      query.sendReply(entry.getData());
    }
  }
}

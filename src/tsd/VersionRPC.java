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
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
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
      final String[] cmd, final TSDFormatter formatter) {
    return formatter.handleTelnetVersion(cmd, chan, this.getVersion());
  }
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    formatter.handleHTTPVersion(query, this.getVersion());
  }
  
  /**
   * Builds a hashmap with various items pertaining to the version
   * @return A hashmap of version information
   */
  private final HashMap<String, Object> getVersion(){
    HashMap<String, Object> version = new HashMap<String, Object>();
    version.put("short_revision", BuildData.short_revision);
    version.put("full_revision", BuildData.full_revision);
    version.put("timestamp", BuildData.timestamp);
    version.put("repo_status", BuildData.repo_status);
    version.put("user", BuildData.user);
    version.put("host", BuildData.host);
    version.put("repo", BuildData.repo);
    return version;
  }
}

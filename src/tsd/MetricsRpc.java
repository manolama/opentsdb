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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.TSDB;

/**
 * STUB
 *
 */
class MetricsRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsRpc.class);

  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && query.getCacheAndReturn(query_hash)){
      return;
    }
    final JSON_HTTP response;
    
    // TEMP STUB
    final String jsonp = JSON_HTTP.getJsonPFunction(query);
    if (query.hasQueryStringParam("tagk"))
      response = new JSON_HTTP(tsdb.getTagNames());
    else if (query.hasQueryStringParam("tagv"))
      response = new JSON_HTTP(tsdb.getTagValues());
    else
      response = new JSON_HTTP(tsdb.getMetrics());
    
    // build our cache object, store and reply
    CacheEntry entry = new CacheEntry(query_hash, 
        jsonp.isEmpty() ? response.getJsonString().getBytes() 
            : response.getJsonPString(jsonp).getBytes(),
            30);
    if (!nocache && !query.putCache(entry)){
      LOG.warn("Unable to cache [" + query_hash + "]");
    }
    query.sendReply(entry.getData());
  }
  
}

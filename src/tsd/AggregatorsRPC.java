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

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;

/**
 * Handles aggregation function information fetching
 */
final class AggregatorsRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatorsRPC.class);
  
  /**
   * Returns a list of aggregation functions supported by the TSD
   * in JSON format
   * @param tsdb Not used in this case
   * @param query HTTP query to respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && query.getCache().readCache(query_hash, query)){
      return;
    }
    
    final String jsonp = JsonHelper.getJsonPFunction(query);
    final JsonHelper response = new JsonHelper(Aggregators.set());
 
    // build our cache object, store and reply
    HttpCacheEntry entry = new HttpCacheEntry(query_hash, 
        jsonp.isEmpty() ? response.getJsonString().getBytes() 
            : response.getJsonPString(jsonp).getBytes(),
            "", /* don't bother persisting these */
            false,
            86400);
    if (!nocache && !query.getCache().storeCache(entry)){
      LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
    }
    query.sendReply(entry.getData());
  }
}

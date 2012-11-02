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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.cache.Cache.CacheRegion;
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
    if (!nocache){
      try{
        @SuppressWarnings("unchecked")
        Set<String> cached = (Set<String>)tsdb.cache.get(CacheRegion.GENERAL, query_hash);
        if (cached != null){
          query.formatter.handleHTTPAggregators(query, cached);
          return;
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    }

    Set<String> aggs = Aggregators.getAggregators();
    if (!nocache)
      tsdb.cache.put(CacheRegion.GENERAL, query_hash, aggs);
    query.formatter.handleHTTPAggregators(query, aggs);
  }
}

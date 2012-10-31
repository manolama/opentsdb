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

import java.util.List;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;

/**
 * The auto-complete oracle class that returns a list of search results
 * for GUIs as users type in a request. This will be hit a lot so it makes
 * sense to cache the results. Results are returned as JSON data
 */
final class SuggestRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(SuggestRPC.class);

  /**
   * Returns auto-complete data for HTTP queries in JSON format
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && query.getCacheAndReturn(query_hash)){
      return;
    }
    
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    // build up the suggestion
    final String type = query.getRequiredQueryStringParam("type");
    final String q = query.getQueryStringParam("q");
    if (q == null) {
      throw BadRequestException.missingParameter("q");
    }
    List<String> suggestions;
    if ("metrics".equals(type)) {
      suggestions = tsdb.suggestMetrics(q);
    } else if ("tagk".equals(type)) {
      suggestions = tsdb.suggestTagNames(q);
    } else if ("tagv".equals(type)) {
      suggestions = tsdb.suggestTagValues(q);
    } else {
      throw new BadRequestException("Invalid 'type' parameter:" + type);
    }
    formatter.handleHTTPSuggest(query, suggestions);
  }
}

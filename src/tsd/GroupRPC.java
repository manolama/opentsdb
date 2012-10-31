// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchResults;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(GroupRPC.class);
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (tsdb.role != TSDRole.API){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + tsdb.role + "]");
      return;
    }
    
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    // parse the search query
    SearchQuery search_query = new SearchQuery();
    if (query.getMethod() == HttpMethod.POST){
      LOG.trace("Parsing POST data");
      JSON codec = new JSON(search_query);
      if (!codec.parseObject(query.getPostData())){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data");
        return;
      }
      search_query = (SearchQuery)codec.getObject();
    }else{
      LOG.trace("Parsing query string data");
      if (!search_query.parseQueryString(query))
        return;
    }
    
    // validate the query first
    if (!search_query.validateQuery()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    JSON codec = new JSON(search_query);
    LOG.trace(codec.getJsonString());

    SearchResults results = null;
    if (search_query.getTerms())
      results = tsdb.meta_searcher.getTerms(search_query);
    else
      results = tsdb.meta_searcher.groupBy(search_query);
    if (results == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    results.time = ((double)(System.nanoTime() - query.start_time) / (double)1000000);
    formatter.handleHTTPGroupby(query, results);
    return;
  }

}

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

import java.io.IOException;
import java.util.ArrayList;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchResults;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

/**
 * Handles search requests for different timeseries metadata.
 * 
 * NOTE: This does NOT return actual data points, just lets users browse
 * information about the data that's stored in the system.
 *
 */
public class SearchRPC implements HttpRpc {  
  private static final Logger LOG = LoggerFactory.getLogger(SearchRPC.class);
    
  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (TSDB.role == TSDRole.Ingest){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + TSDB.role + "]");
      return;
    }
    
    String endpoint = query.getEndpoint();
    
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    // parse the search query
    SearchQuery search_query = new SearchQuery();
    if (query.getMethod() == HttpMethod.POST){
      try {
        search_query = (SearchQuery)JSON.parseToObject(query.getPostData(), SearchQuery.class);
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else{
      LOG.trace("Parsing query string data");
      search_query.parseQueryString(query);
      if (!search_query.getError().isEmpty())
        return;
    }
    
    // validate the query first
    if (!search_query.validateQuery()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    SearchResults results = null;
    if (endpoint != null && endpoint.toLowerCase().compareTo("annotations") == 0){
      //results = tsdb.search_handler.getAnnotations(search_query);
    } else
      results = tsdb.search_handler.searchTSMeta(search_query);
    if (results == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    results.time = ((double)(System.nanoTime() - query.start_time) / (double)1000000);
    query.getFormatter().handleHTTPSearch(query, results);
  }
}

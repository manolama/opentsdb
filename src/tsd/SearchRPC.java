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

import java.util.ArrayList;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchResults;
import net.opentsdb.uid.UniqueId;

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
      LOG.trace("Parsing POST data");
      JSON codec = new JSON(search_query);
      if (!codec.parseObject(query.getPostData())){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data");
        return;
      }
      search_query = (SearchQuery)codec.getObject();
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
    
    JSON codec = new JSON(search_query);
    LOG.trace(codec.getJsonString());
 
    SearchResults results = null;
    if (endpoint != null && endpoint.toLowerCase().compareTo("annotations") == 0)
      results = tsdb.annotation_searcher.getAnnotations(search_query);
    else if (search_query.getReturnTSUIDs())
      results = tsdb.meta_searcher.searchTSUIDs(search_query);
    else if (search_query.getReturnMeta()){
      SearchResults tsuids = tsdb.meta_searcher.searchTSUIDs(search_query);
      if (tsuids.tsuids == null){
        LOG.warn("No timeseres found to load metadata for");
      }else{
        ArrayList<TimeSeriesMeta> metas = new ArrayList<TimeSeriesMeta>();
        
        for (String tsuid : tsuids.tsuids){
          TimeSeriesMeta tmeta = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(tsuid));
          if (tmeta == null){
            LOG.warn(String.format("Unable to load metadata for [%s]", tsuid));
            continue;
          }
          metas.add(tmeta);
        }
        results = new SearchResults(search_query);
        results.ts_meta = metas;
      }
    }else
      results = tsdb.meta_searcher.searchShortMeta(search_query);
    if (results == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    results.time = ((double)(System.nanoTime() - query.start_time) / (double)1000000);
    query.getFormatter().handleHTTPSearch(query, results);
  }
}

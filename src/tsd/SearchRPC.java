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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchOperator;
import net.opentsdb.uid.NoSuchUniqueId;
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
    
  @SuppressWarnings("unchecked")
  public void execute(final TSDB tsdb, final HttpQuery query) {
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
      search_query = this.parseQueryString(query);
      // error already sent
      if (search_query == null)
        return;
    }
    
    // validate the query first
    if (!search_query.validateQuery()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    JSON codec = new JSON(search_query);
    LOG.trace(codec.getJsonString());
 
    ArrayList<Map<String, Object>> results = tsdb.meta_searcher.searchShortMeta(search_query);
    if (results == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    // build a response map and send away!
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", search_query.getLimit());
    response.put("page", search_query.getPage());
    response.put("total_uids", search_query.getTotal_hits());
    response.put("total_pages", search_query.getPages());
    response.put("results", results);
    codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return;
  }
  
  /**
   * Parses the HTTP query string for the search query
   * @param query HTTP query to parse
   * @return A SearchQuery object if parsed successfully or NULL if there was an error
   * parsing a numeric field
   */
  private SearchQuery parseQueryString(final HttpQuery query){
    SearchQuery sq = new SearchQuery();
    
    if (query.hasQueryStringParam("tags"))
      sq.setTags(this.parseQueryStringList(query.getQueryStringParam("tags")));
    
    if (query.hasQueryStringParam("custom"))
      sq.setCustom(this.parseQueryStringList(query.getQueryStringParam("custom")));
    
    sq.setCreated(query.getQueryStringParam("created"));
    sq.setRetention(query.getQueryStringParam("retention"));
    sq.setMax(query.getQueryStringParam("max"));
    sq.setMin(query.getQueryStringParam("min"));
    sq.setInterval(query.getQueryStringParam("interval"));
    sq.setLast_received(query.getQueryStringParam("last_received"));
    sq.setField(query.getQueryStringParam("field"));
    sq.setQuery(query.getQueryStringParam("query"));
    sq.setReturnMeta(query.parseBoolean(query.getQueryStringParam("return_meta")));
    if (query.hasQueryStringParam("limit")){
      try{
        sq.setLimit(Integer.parseInt(query.getQueryStringParam("limit")));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return null;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        sq.setPage(Integer.parseInt(query.getQueryStringParam("page")));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the page value");
        return null;
      }
    }
    return sq;
  }
 
  /**
   * Parses a query string parameter for key/value pairs, used for the tags and custom
   * The string should be in the format {tag1=val1,tag2=val2}
   * @param query Value of the query string to parse
   * @return Null if there was an error, a map if kvs were parsed successfully
   */
  private Map<String, String> parseQueryStringList(final String query){
    if (query == null || query.isEmpty())
      return null;
    
    final int curly = query.indexOf('{');
    String list = (curly >= 0 ? query.substring(curly+1) : query);
    list = list.replaceAll("}", "");
    
    String[] pairs = null;
    if (list.indexOf(",") < 0)
      pairs = new String[]{list};
    else
      pairs = list.split(",");
    if (pairs == null){
      LOG.warn("Unable to extract any pairs from the query string");
      return null;
    }
    
    Map<String, String> kvs = new HashMap<String, String>();
    for (String pair : pairs){
      if (pair.indexOf("=") < 0){
        LOG.warn("Missing = sign");
        continue;
      }
      
      String[] kv = pair.split("=");
      if (kv.length != 2){
        LOG.warn("Invalid tag pair, wrong number of values [" + kv.length + "]");
        continue;
      }
      kvs.put(kv[0], kv[1]);
    }
    return kvs;
  }
}

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;

import org.codehaus.jackson.type.TypeReference;
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

    Map<String, Object> results = tsdb.meta_searcher.groupBy(search_query);
    if (results == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, search_query.getError());
      return;
    }
    
    double time = ((double)(System.nanoTime() - query.start_time) / (double)1000000);
    
    // build a response map and send away!
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", search_query.getLimit());
    response.put("page", search_query.getPage());
    response.put("total_uids", search_query.getTotal_hits());
    response.put("total_pages", search_query.getPages());
    response.put("time", time);
    response.put("total_groups", search_query.getTotalGroups());
    response.put("results", results);
    codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return;
  }
  
  private static class TsdbGroup{
    public SortedMap<String, List<Map<String, Object>>> groups;
    public long total_uids = 0;
    public long largest_group = 0;
  }
}

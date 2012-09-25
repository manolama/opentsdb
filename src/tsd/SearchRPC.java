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
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdMap;

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
    if (query.getMethod() == HttpMethod.POST){
      // todo - handle me!!!
    }
    
    if (tsdb.ts_uids.isEmpty()){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Timeseries UID list is empty");
      return;
    }
    
    SearchQuery search_query = new SearchQuery();
    if (query.getMethod() == HttpMethod.POST){
      JSON codec = new JSON(search_query);
      if (!codec.parseObject(query.getPostData())){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data");
        return;
      }
    }else{
      search_query = this.parseQueryString(query);
      // error already sent
      if (search_query == null)
        return;
    }
     
    // results will be stored by UID and object to avoid duplication
    Map<String, Object> results = new HashMap<String, Object>();
    Set<String> tag_pairs = new HashSet<String>();
    Set<String> metrics = new HashSet<String>();
    
    Pattern reg_query = Pattern.compile(search_query.query);
    
    Map<String, Pattern> tags = new HashMap<String, Pattern>();
    for (Map.Entry<String, String> entry : search_query.tags.entrySet()){
      Pattern tagv_pattern = Pattern.compile(entry.getValue());
      tags.put(entry.getKey(), tagv_pattern);
    }
    
    // if all, then run regex against:
    // metric names
    // tagk names
    // tagv names
    
    tsdb.metrics.LoadAllMeta();
    tsdb.tag_names.LoadAllMeta();
    tsdb.tag_values.LoadAllMeta();
    LOG.trace("Loaded all of the general meta objects");
    
    tsdb.metrics.searchNames(reg_query, metrics);
    tsdb.tag_names.searchNames(reg_query, tag_pairs);
    tsdb.tag_values.searchNames(reg_query, tag_pairs);
    
    // with the list of tagpairs, we can get a list of TSUIDs
    Set<String> tsuids = UniqueIdMap.getTSUIDs(tag_pairs, tsdb.ts_uids, (short)3);
    LOG.trace(String.format("Found [%d] tsuids with [%d] tag pairs", 
        tsuids.size(), tag_pairs.size()));
    
    // match metrics
    if (metrics.size() > 0){
      for (String tsuid : tsdb.ts_uids){
        for (String metric : metrics){
          if (tsuid.substring(0, 6).compareTo(metric) == 0)
            tsuids.add(tsuid);
        }
      }
    }
    Map<String, Object> tag_pair_map = new HashMap<String, Object>();
    int count=0;
    for (String tsuid : tsuids){
      //LOG.trace(tsuid);
      try{
        // explode tags
        List<String> pairs = new ArrayList<String>();
        for (int i = 6; i<tsuid.length(); i+=12){
          pairs.add(tsuid.substring(i, i + 12));
        }
        try{
          
          // if we have tags, checkem!
          for (Map.Entry<String, Pattern> entry : tags.entrySet()){
            
          }
        
          // all good, so store it!

          if (search_query.return_meta)
            results.put(tsuid, tsdb.getTimeSeriesMeta(UniqueId.StringtoID(tsuid)));
          else
            results.put(tsuid, tsdb.getTSUIDShortMeta(tsuid));
        } catch (NoSuchUniqueId nsui){
          LOG.trace(nsui.getMessage());
        }
      }catch (NoSuchUniqueId nsui){
        LOG.debug(String.format("No UID for [%s]", tsuid));
      }
      
      count++;
    }
    
    // build a response map
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", search_query.limit);
    response.put("page", search_query.page);
    response.put("total_uids", results.size());
    response.put("total_pages", ((results.size() / search_query.limit) + 1));
    
    // limit and page calculations    
    if (search_query.limit < 1 || results.size() <= search_query.limit){
      response.put("results", results.values());
    }else{
      LOG.trace(String.format("Limiting to [%d] uids on page [%d]", search_query.limit, search_query.page));
      SortedMap<String, Object> limited_results = new TreeMap<String, Object>();
      long start = (search_query.page > 0 ? search_query.page * search_query.limit : 0);
      long end = (search_query.page + 1) * search_query.limit;
      long uids = 0;
      for (Map.Entry<String, Object> entry : results.entrySet()){
        if (uids >= end){
          LOG.trace(String.format("Hit the limit [%d] with [%d] uids", 
              end, uids));
          break;
        }
        if (uids >= start && uids <= end){
          limited_results.put(entry.getKey(), entry.getValue());
        }
        uids++;
      }
      response.put("results", limited_results);
    }   
    JSON codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return;
  }
  
  private SearchQuery parseQueryString(final HttpQuery query){
    SearchQuery sq = new SearchQuery();
    
    // todo - tags and custom
    
    sq.created = query.getQueryStringParam("created");
    sq.retention = query.getQueryStringParam("retention");
    sq.max = query.getQueryStringParam("max");
    sq.min = query.getQueryStringParam("min");
    sq.interval = query.getQueryStringParam("interval");
    sq.first_received = query.getQueryStringParam("first_received");
    sq.last_received = query.getQueryStringParam("last_received");
    sq.field = query.getQueryStringParam("field").toLowerCase();
    sq.query = query.getQueryStringParam("query");
    sq.return_meta = query.parseBoolean(query.getQueryStringParam("return_meta"));
    if (query.hasQueryStringParam("limit")){
      try{
        sq.limit = Long.parseLong(query.getQueryStringParam("limit"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return null;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        sq.page = Long.parseLong(query.getQueryStringParam("page"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the page value");
        return null;
      }
    }
    
    
    return sq;
  }
  
  @SuppressWarnings("unused")
  private static class SearchQuery{
    public Map<String, String> tags;
    public Map<String, String> custom;
    public String created;
    public String retention;
    public String max;
    public String min;
    public String interval;
    public String first_received;
    public String last_received;
    public String field;
    public String query;
    public long limit = 25;
    public long page = 0;
    public Boolean return_meta = false;
  }
}

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
import net.opentsdb.core.SearchQuery.SearchOperator;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.SearchQuery;
import net.opentsdb.meta.TimeSeriesMeta;
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
    
  @SuppressWarnings("unchecked")
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // don't bother if we don't have any tsuids to work with
    if (tsdb.ts_uids.stringSize() < 1){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Timeseries UID list is empty");
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
    
    Map<String, Object> results = null;
    
    // try retrieving/parsing the cache
    CacheEntry cached = query.getCache(search_query.hashCode());
    if (cached != null){
      final TypeReference<Map<String, Object>> respTypeRef = 
        new TypeReference<Map<String, Object>>() {}; 
      codec = new JSON(results);
      if (!codec.parseObject(
          (cached.getDataSize() > 0 ? cached.getData() : cached.getFileData()), respTypeRef))
        cached = null;
      else
        results = (Map<String, Object>)codec.getObject();
    }
    
    if (cached == null){
      // results will be stored by UID and object to avoid duplication
      Set<String> tag_pairs = new HashSet<String>();
      Set<String> metrics = new HashSet<String>();
  
      // only search the metrics and tags we need to so we save some CPU time
      if (search_query.searchingGeneralMeta() || 
          search_query.getField().compareTo("metrics") == 0)
        tsdb.metrics.search(search_query, metrics);
      if (search_query.searchingGeneralMeta() || 
          search_query.getField().compareTo("tagk") == 0)
        tsdb.tag_names.search(search_query, tag_pairs);
      if (search_query.searchingGeneralMeta() || 
          search_query.getField().compareTo("tagv") == 0)
        tsdb.tag_values.search(search_query, tag_pairs);
      
      // with the list of tagpairs, we can get a list of matching tsuids from the master
      Set<String> tsuids = null;/*tsdb.ts_uids.getTSUIDs(tag_pairs, tsdb.ts_uids.get(), (short)3);
      LOG.trace(String.format("Found [%d] tsuids with [%d] tag pairs", 
          tsuids.size(), tag_pairs.size()));
      
      // match metrics against the timeseries list and add to the tsuids set
      if (metrics.size() > 0){
        HashSet<String> uids = tsdb.ts_uids.get();
        for (String tsuid : uids){
          for (String metric : metrics){
            if (tsuid.substring(0, 6).compareTo(metric) == 0)
              tsuids.add(tsuid);
          }
        }
      }
      */
  
      // store a quick flag for efficiency
      boolean searching_meta = search_query.searchingAnyMeta();
      
      // if we're searching meta, we need to process timeseries meta as well
      if (searching_meta)
        tsdb.searchTSMeta(search_query, tsuids);
  
      // loop through the tsuids, load meta if applicable, load short_meta, and then filter
      // and store the results in the results hash
      results = new HashMap<String, Object>();
      for (String tsuid : tsuids){
        TimeSeriesMeta ts_meta = null;
        Map<String, Object> short_meta = null;
        try{
          // load the meta if we're returning or searching
          if (search_query.getReturnMeta() || searching_meta)
            ts_meta = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(tsuid));
          // always load the short meta since we'll likely return this only and it has tags
          short_meta = tsdb.getTSUIDShortMeta(tsuid);
          
        } catch (NoSuchUniqueId nsui){
          LOG.trace(nsui.getMessage());
          continue;
        }
        
        // FILTER - TAG
        if (search_query.getTagsCompiled() != null){
          LOG.trace("Filtering tags...");
          Map<String, String> sm_tags = (Map<String, String>)short_meta.get("tags");
          int matched = 0;
          for (Map.Entry<String, Pattern> entry : search_query.getTagsCompiled().entrySet()){
            for (Map.Entry<String, String> tag : sm_tags.entrySet()){
              if (tag.getKey().toLowerCase().compareTo(entry.getKey().toLowerCase()) == 0
                  && entry.getValue().matcher(tag.getValue()).find()){
                LOG.trace(String.format("Matched tag [%s] on filter [%s] with value [%s]",
                    tag.getKey(), entry.getValue().toString(), tag.getValue()));
                matched++;
              }
            }
          }
          if (matched != search_query.getTagsCompiled().size()){
            LOG.trace(String.format("UID [%s] did not match all of the tag filters", tsuid));
            continue;
          }
        }
        
        // FILTER - Numerics
        if (search_query.getNumerics() != null && ts_meta != null){
          int matched = 0;
          for (Map.Entry<String, SimpleEntry<SearchOperator, Double>> entry : 
            search_query.getNumerics().entrySet()){
            Double meta_value;
            if (entry.getKey().compareTo("retention") == 0)
              meta_value = (double) ts_meta.getRetention();
            else if (entry.getKey().compareTo("max") == 0)
              meta_value = (double) ts_meta.getMax();
            else if (entry.getKey().compareTo("min") == 0)
              meta_value = (double) ts_meta.getMin();
            else if (entry.getKey().compareTo("interval") == 0)
              meta_value = (double) ts_meta.getInterval();
            else if (entry.getKey().compareTo("created") == 0)
              meta_value = (double) ts_meta.getFirstReceived();
            else if (entry.getKey().compareTo("last_received") == 0)
              meta_value = (double) ts_meta.getLastReceived();
            else{
              LOG.warn(String.format("Invalid field type [%s]", entry.getKey()));
              continue;
            }
            
            // get comparator and value
            SearchOperator operator = entry.getValue().getKey();
            Double comparisson = entry.getValue().getValue();
            if (operator == null || comparisson == null){
              LOG.warn("Found a null operator or comparisson value");
              continue;
            }
            
            // compare
            if (search_query.compare(operator, meta_value, comparisson))
              matched++;
          }
          if (matched != search_query.getNumerics().size()){
            //LOG.trace(String.format("UID [%s] did not match all of the numeric filters", tsuid));
            continue;
          }
        }
  
        // Put the result!
        if (search_query.getReturnMeta())
          results.put(tsuid, ts_meta);
        else
          results.put(tsuid, short_meta);
      }
      
      // cache me
      codec = new JSON(results);
      CacheEntry ce = new CacheEntry(search_query.hashCode(), codec.getJsonBytes());
      query.putCache(ce);
    }

    // build a response map and send away!
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", search_query.getLimit());
    response.put("page", search_query.getPage());
    response.put("total_uids", results.size());
    response.put("total_pages", results.size() > 0 ? 
        ((results.size() / search_query.getLimit()) + 1) : 0);
    
    // limit and page calculations    
    if (search_query.getLimit() < 1 || results.size() <= search_query.getLimit()){
      response.put("results", results.values());
    }else{
      LOG.trace(String.format("Limiting to [%d] uids on page [%d]", search_query.getLimit(), search_query.getPage()));
      SortedMap<String, Object> limited_results = new TreeMap<String, Object>();
      long start = (search_query.getPage() > 0 ? search_query.getPage() * search_query.getLimit() : 0);
      long end = (search_query.getPage() + 1) * search_query.getLimit();
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
        sq.setLimit(Long.parseLong(query.getQueryStringParam("limit")));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return null;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        sq.setPage(Long.parseLong(query.getQueryStringParam("page")));
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

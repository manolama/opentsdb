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
import java.util.regex.Pattern;

import org.jboss.netty.handler.codec.http.HttpMethod;
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
    
    String field = "all";
    String search = "";
    Boolean return_meta = false;
    
    // load query string
    if (query.hasQueryStringParam("field"))
      field = query.getQueryStringParam("field").toLowerCase();
    if (query.hasQueryStringParam("query"))
      search = query.getQueryStringParam("query");
    if (query.hasQueryStringParam("return_meta"))
      return_meta = query.parseBoolean(query.getQueryStringParam("return_meta"));
    
    // checks!!
    
    // results will be stored by UID and object to avoid duplication
    Map<String, Object> results = new HashMap<String, Object>();
    Set<String> tag_pairs = new HashSet<String>();
    Set<String> metrics = new HashSet<String>();
    
    Pattern reg_query = Pattern.compile(search);
    
    // if all, then run regex against:
    // Storage JSON
    // metric names
    // tagk names
    // tagv names
    
    tsdb.metrics.searchNames(reg_query, return_meta, metrics);
    tsdb.tag_names.searchNames(reg_query, return_meta, tag_pairs);
    tsdb.tag_values.searchNames(reg_query, return_meta, tag_pairs);
    
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

        Map<String, Object> ts = new HashMap<String, Object>();
        ts.put("uid", tsuid);
        ts.put("metric", tsdb.metrics.getName(UniqueId.StringtoID(tsuid.substring(0, 6))));
        
        Map<String, String> kv = new HashMap<String, String>();
        for (String pair : pairs){
          Map<String, String> p = (Map<String, String>)tag_pair_map.get(pair);
          if (p != null){
            kv.put(p.get("key"), p.get("value"));
            continue;
          }
          try{
            
            String t = tsdb.tag_names.getName(UniqueId.StringtoID(pair.substring(0, 6)));
            String v = tsdb.tag_values.getName(UniqueId.StringtoID(pair.substring(6)));
            kv.put(t, v);
            p = new HashMap<String, String>();
            p.put("key", t);
            p.put("value", v);
            tag_pair_map.put(pair, p);
          } catch (NoSuchUniqueId nsui){
            LOG.debug(String.format("No UID for [%s]", tsuid));
          }
        }
        ts.put("tags", kv);
        
        results.put(tsuid, ts);
      }catch (NoSuchUniqueId nsui){
        LOG.debug(String.format("No UID for [%s]", tsuid));
      }
      
      count++;
//      if (count > 5)
//        break;
    }
    
    // build a response map
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", 0);
    response.put("page", 0);
    response.put("total_uids", results.size());
    response.put("results", results.values());
    
    JSON codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return;
  }
}

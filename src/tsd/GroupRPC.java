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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import net.opentsdb.cache.CacheEntry;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdMap;

import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(GroupRPC.class);
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    
    String grouping_tagk = "host";
    long limit = 25;
    long page = 0;
    
    // load query string
    if (query.hasQueryStringParam("tag"))
      grouping_tagk = query.getQueryStringParam("tag").toLowerCase();
    if (query.hasQueryStringParam("limit")){
      try{
      limit = Long.parseLong(query.getQueryStringParam("limit"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        page = Long.parseLong(query.getQueryStringParam("page"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the page value");
        return;
      }
    }

    TsdbGroup results = null;
    JSON codec;
    
    // build a hash
    int hash = ("group" + grouping_tagk).hashCode();
    
    // try retrieving/parsing the cache
    CacheEntry cached = query.getCache(hash);
    if (cached != null){
      final TypeReference<TsdbGroup> respTypeRef = 
        new TypeReference<TsdbGroup>() {}; 
      codec = new JSON(results);
      if (!codec.parseObject(
          (cached.getDataSize() > 0 ? cached.getData() : cached.getFileData()), respTypeRef))
        cached = null;
      else
        results = (TsdbGroup)codec.getObject();
    }

    if (cached == null){
      byte[] uid;
      try{
       uid = tsdb.tag_names.getId(grouping_tagk);
      } catch (NoSuchUniqueId nsui){
        LOG.debug(String.format("No tagk UID for [%s]", grouping_tagk));
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to locate tag name");
        return;
      }
      String tagk = UniqueId.IDtoString(uid);
      
      UniqueIdMap map = tsdb.tag_names.getMap(tagk);
      if (map == null){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to load map");
        return;
      }
      
      Set<String> tagvs = map.getTags("tagv", (short)3);
      if (tagvs == null){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Error fetching tagv set");
        return;
      }
      
      // sort em!
      SortedMap<String, String> sorted_tagvs = new TreeMap<String, String>();
      for (String tagvid : tagvs){
        try{
          sorted_tagvs.put(tsdb.tag_values.getName(UniqueId.StringtoID(tagvid)), tagvid);
         }catch (NoSuchUniqueId nsui){
           LOG.trace(String.format("No tagv UID for [%s]", tagvid));
           //query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to locate tag name");
           continue;
         }
      }
      if (sorted_tagvs.size() < 1){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Failed to sort tag values");
        return;
      }
      
      results = new TsdbGroup();
      results.groups = new TreeMap<String, List<Map<String, Object>>>();
      
      // fetch TSUIDs for each pair
      results.total_uids = 0;
      for (Map.Entry<String, String> entry : sorted_tagvs.entrySet()){
        List<Map<String, Object>> entries = new ArrayList<Map<String, Object>>();
        for (String tsuid : tsdb.ts_uids){
          if (tsuid.substring(6).contains(tagk + entry.getValue())){
            //LOG.trace(String.format("Matched [%s] with [%s]", tagk + entry.getValue(), tsuid));
            try{
              entries.add(tsdb.getTSUIDShortMeta(tsuid));
              results.total_uids++;
            } catch (NoSuchUniqueId nsui){
              LOG.trace(nsui.getMessage());
            }
          }
        }
        if (entries.size() > 0){
          results.groups.put(entry.getKey(), entries);
          //LOG.trace(String.format("Put [%s]", entry.getKey()));
          if (entries.size() > results.largest_group)
            results.largest_group = entries.size();
        }
      }
      
      // cache me
      codec = new JSON(results);
      CacheEntry ce = new CacheEntry(hash, codec.getJsonBytes());
      query.putCache(ce);
    }
    
    // if one group is larger than the limit, we need to reset the limit
    if (results.largest_group > limit){
      LOG.warn(String.format("Resetting limit from [%d] to largest group size [%d]", limit, results.largest_group));
      limit = results.largest_group;
    }
    
    // build a response map
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", limit);
    response.put("page", page);
    response.put("total_uids", results.total_uids);
    response.put("total_groups", results.groups.size());
    response.put("tagk", grouping_tagk);
    response.put("total_pages", ((results.total_uids / limit) + 1));
    
    // limit and page calculations    
    if (limit < 1 || results.total_uids <= limit){
      response.put("results", results.groups);
    }else{
      SortedMap<String, Object> limited_results = new TreeMap<String, Object>();
      long start = (page > 0 ? page * limit : 0);
      long end = (page + 1) * limit;
      long total_uids = 0;
      LOG.trace(String.format("Limiting to [%d] uids on page [%d] w start [%d] end [%d]", limit, page, start, end));
      for (Map.Entry<String, List<Map<String, Object>>> entry : results.groups.entrySet()){
        long size = entry.getValue().size();
        if (total_uids + size > end){
          LOG.trace(String.format("[%d] Next group with [%d] would hit limit [%d] since we have [%d]", 
              total_uids, size, end, total_uids));
          break;
        }
        if ((total_uids + size) >= start && (total_uids + size) <= end)
          limited_results.put(entry.getKey(), entry.getValue());

        total_uids += size;
      }
      response.put("results", limited_results);
    }   
    
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

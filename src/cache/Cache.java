package net.opentsdb.cache;

//This file is part of OpenTSDB.
//Copyright (C) 2012  The OpenTSDB Authors.
//
//This program is free software: you can redistribute it and/or modify it
//under the terms of the GNU Lesser General Public License as published by
//the Free Software Foundation, either version 3 of the License, or (at your
//option) any later version.  This program is distributed in the hope that it
//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
//General Public License for more details.  You should have received a copy
//of the GNU Lesser General Public License along with this program.  If not,
//see <http://www.gnu.org/licenses/>.

import java.util.LinkedList;
import java.util.ListIterator;

import net.opentsdb.core.TsdbConfig;
import net.opentsdb.stats.StatsCollector;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.admin.CacheRegionInfo;
import org.apache.jcs.admin.JCSAdminBean;
import org.apache.jcs.engine.behavior.IElementAttributes;
import org.apache.jcs.engine.control.CompositeCache;
import org.apache.jcs.engine.control.CompositeCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Handles caching for HTTP queries It automatically handles
* persisting cache objects to disk and removes them when they expire. It is
* also able to hold small objects in RAM for quick access but if the configured
* RAM limit is reached, it will try to grab data from disk.
*/
public class Cache {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);
  
  public enum CacheRegion {
      QUERY,        /** for data query results */
      SEARCH,       /** for search query results */
      META,         /** for metadata */
      GENERAL       /** for general use */
  };
  
  private CompositeCacheManager ccm = CompositeCacheManager.getUnconfiguredInstance();
  private JCS query_cache = null;
  private JCS search_cache = null;
  private JCS meta_cache = null;
  private JCS general_cache = null;  
  
  public Cache(final TsdbConfig config) {
    try {
      ccm.configure(config.getProperties());
      
      this.query_cache = JCS.getInstance("query");
      this.search_cache = JCS.getInstance("search");
      this.meta_cache = JCS.getInstance("meta");
      this.general_cache = JCS.getInstance("general");
    } catch (CacheException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public boolean put(final CacheRegion region, final Object key, final Object value) {
    try{
      switch (region){
      case QUERY:
        if (this.query_cache == null)
          return false;
        this.query_cache.put(key, value);
        return true;
        
      case SEARCH:
        if (this.search_cache == null)
          return false;
        this.search_cache.put(key, value);
        return true;
        
      case META:
        if (this.meta_cache == null)
          return false;
        this.meta_cache.put(key, value);
        return true;
        
      case GENERAL:
        if (this.general_cache == null)
          return false;
        this.general_cache.put(key, value);
        return true;
      
      default:
          LOG.error("Unrecognized enumerator");
          return false;
      }
    } catch (CacheException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }
  
  public boolean put(final CacheRegion region, final Object key, final Object value, final int max_life) {
    try{
      IElementAttributes attributes = null;
      switch (region){
      case QUERY:
        if (this.query_cache == null)
          return false;
        attributes = query_cache.getDefaultElementAttributes();
        attributes.setMaxLifeSeconds(max_life);
        this.query_cache.put(key, value, attributes);
        return true;
        
      case SEARCH:
        if (this.search_cache == null)
          return false;
        attributes = search_cache.getDefaultElementAttributes();
        attributes.setMaxLifeSeconds(max_life);
        this.search_cache.put(key, value, attributes);
        return true;
        
      case META:
        if (this.meta_cache == null)
          return false;
        attributes = meta_cache.getDefaultElementAttributes();
        attributes.setMaxLifeSeconds(max_life);
        this.meta_cache.put(key, value, attributes);
        return true;
        
      case GENERAL:
        if (this.general_cache == null)
          return false;
        attributes = general_cache.getDefaultElementAttributes();
        attributes.setMaxLifeSeconds(max_life);
        this.general_cache.put(key, value, attributes);
        return true;
      
      default:
          LOG.error("Unrecognized enumerator");
          return false;
      }
    } catch (CacheException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }
 
  public Object get(final CacheRegion region, final Object key){
    switch (region){
    case QUERY:
      if (this.query_cache == null)
        return null;
      return this.query_cache.get(key);
      
    case SEARCH:
      if (this.search_cache == null)
        return null;
      return this.search_cache.get(key);
      
    case META:
      if (this.meta_cache == null)
        return null;
      return this.meta_cache.get(key);
      
    case GENERAL:
      LOG.trace("Trying to get [" + key.toString() + "] from general");
      if (this.general_cache == null){
        LOG.trace("Not found");
        return null;
      }
      return this.general_cache.get(key);
    
    default:
        LOG.error("Unrecognized enumerator");
        return null;
    }
  }

  public void collectStats(final StatsCollector collector){
    try {
      JCSAdminBean admin = new JCSAdminBean();
      LinkedList linkedList  = admin.buildCacheInfo();
      ListIterator iterator = linkedList.listIterator();

      while (iterator.hasNext()) {
        CacheRegionInfo info = (CacheRegionInfo)iterator.next();
        CompositeCache compCache = info.getCache();
        String base = "cache." + compCache.getCacheName() +".";
        
//        System.out.println("Cache Name: " + compCache.getCacheName());
//        System.out.println("Cache Type: " + compCache.getCacheType());
        System.out.println("Cache Misses (not found): " + compCache.getMissCountNotFound());
        System.out.println("Cache Misses (expired): " + compCache.getMissCountExpired());
        System.out.println("Cache Hits (memory): " + compCache.getHitCountRam());
        System.out.println("Cache Updates: " + compCache.getUpdateCount());
        
        collector.record(base + "misses.notfound", compCache.getMissCountNotFound());
        collector.record(base + "misses.expired", compCache.getMissCountExpired());
        collector.record(base + "hits.memory", compCache.getHitCountRam());
        collector.record(base + "hits.aux", compCache.getHitCountAux());
        collector.record(base + "updates", compCache.getUpdateCount());
        collector.record(base + "items", compCache.getSize());
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
//    collector.record("formatters.storage_errors", storage_errors.get());
//    collector.record("formatters.invalid_values", invalid_values.get());
//    collector.record("formatters.illegal_arguments", illegal_arguments.get());
//    collector.record("formatters.unknown_metrics", unknown_metrics.get());

  }
}


// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles caching for HTTP queries It automatically handles
 * persisting cache objects to disk and removes them when they expire. It is
 * also able to hold small objects in RAM for quick access but if the configured
 * RAM limit is reached, it will try to grab data from disk.
 */
class HttpCache implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(HttpCache.class);

  /** Stores the cache entries */
  private ConcurrentHashMap<Integer, HttpCacheEntry> cache = new ConcurrentHashMap<Integer, HttpCacheEntry>();

  /** How many times cache was fulfilled from RAM */
  private static final AtomicInteger cache_ram_hit = new AtomicInteger();
  /** How many times cache was fulfilled from disk */
  private static final AtomicInteger cache_disk_hit = new AtomicInteger();
  /** How many times an object wasn't found in cache */
  private static final AtomicInteger cache_miss = new AtomicInteger();
  /** How much space is taken up by raw object data (does not include overhead) */
  private static final AtomicInteger cache_ram_size = new AtomicInteger();

  /** Sets the limit we can store in RAM */
  private final int ram_size_limit;
  /** Sets the maximum chunk of data (or file) we can store in RAM */
  private final int ram_file_size_limit;
  /** The {@code TSDB} instance we belong to */
  private final TSDB tsd;

  /**
   * Default constructor
   * @param tsd The TSDB we belong to.
   */
  public HttpCache(final TSDB tsd) {
    this.tsd = tsd;
    ram_size_limit = tsd.getConfig().httpCacheRamLimit();
    ram_file_size_limit = tsd.getConfig().httpCacheRamFileLimit();
  }

  /**
   * Attempts to pull data from the cache, and if successful, returns it to the
   * HTTP query
   * <p>
   * First we see if the object exists in the hash map. If it does, we check to
   * see if it's expired. Then it looks to see if we have data in RAM we can
   * return. Then it sees if the file exists and will try to pull it if so.
   * @param key The query hash
   * @param query Query to respond to if we have data
   * @return True if data was successfully found in the cache, false if not
   */
  public boolean readCache(final int key, final HttpQuery query) {
    HttpCacheEntry entry = cache.get(key);
    if (entry == null) {
      LOG.debug("Unable to find [" + key + "] in cache");
      cache_miss.incrementAndGet();
      return false;
    }

    // increment hits
    entry.incrementHits();

    // see if it's expired
    if (entry.hasExpired()) {
      LOG.debug("Key [" + key + "] has expired");
      deleteCache(key);
      cache_miss.incrementAndGet();
      return false;
    }

    // check the RAM cache
    if (!entry.getFileOnly() && entry.getData() != null) {
      cache_ram_hit.incrementAndGet();
      query.sendReply(entry.getData());
      LOG.debug("Returning data from RAM for [" + key + "]");
      return true;
    }

    // check disk
    String path = entry.getFile();
    if (path.isEmpty()) {
      cache_miss.incrementAndGet();
      LOG.debug("Unable to find [" + key + "] data on disk");
      return false;
    }

    // see if the file exists
    File file = new File(path);
    if (!file.exists()) {
      cache_miss.incrementAndGet();
      LOG.warn("Cache file [" + path + "] is missing");
      return false;
    }

    // send the file
    cache_disk_hit.incrementAndGet();
    try {
      query.sendFile(path, (int) entry.getExpire());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    LOG.debug("Hit disk cache for key [" + key + "]");
    return true;
  }

  /**
   * Attempts to store a HttpCacheEntry object in the cache. This method will
   * automatically handle persisting the data to disk and see if it can be
   * stored in ram (unless overloaded by fileOnly)
   * @param entry An object to store in the cache
   * @return True if we were able to store the data successfully, false if not
   */
  public boolean storeCache(final HttpCacheEntry entry) {
    // TODO make this call asynchronous so queries can return faster
    // if expire == 0 then we are not caching the file
    if (entry.getExpire() < 1) {
      LOG.debug("Entry [" + entry.getKey() + "] is set to not be cached");
      return true;
    }

    boolean delete_ram = false;

    // see if the file is small enough to fit in RAM and if it's not fileOnly
    if (!entry.getFileOnly() && entry.getDataSize() > 0
        && entry.getDataSize() <= ram_file_size_limit) {
      if ((entry.getDataSize() + cache_ram_size.longValue()) > ram_size_limit) {
        if (!freeRAM(entry.getKey()))
          delete_ram = true;
      }
    }

    // flush the bytes to a file if it hasn't been done already
    if (!entry.getFileOnly() && !entry.getFile().isEmpty()
    /* && filecache is enabled */) {
      try {
        LOG.debug("Attempting to save cache file [" + entry.getFile() + "]");
        final FileOutputStream out = new FileOutputStream(entry.getFile());
        try {
          out.write(entry.getData());
          LOG.debug("Stored cache file [" + entry.getFile() + "]");
        } finally {
          out.close();
        }
      } catch (FileNotFoundException e) {
        LOG.error("Failed to create file [" + entry.getFile() + "]");
        e.printStackTrace();
      } catch (IOException e) {
        LOG.error("Failed to write file [" + entry.getFile() + "]");
        e.printStackTrace();
      }
    }

    // free up the ram bit if we are out of space
    if (delete_ram)
      entry.flushData();

    // check to see if we're overwriting an existing cache object
    if (cache.containsKey(entry.getKey()))
      LOG.debug("Overwriting existing key [" + entry.getKey() + "]");

    // store
    if (entry.getDataSize() > 0)
      cache_ram_size.addAndGet(entry.getDataSize());
    cache.put(entry.getKey(), entry);
    LOG.debug("Stored key [" + entry.getKey() + "] in cache");
    return true;
  }

  /**
   * Overload that generates a HttpCacheEntry object and forwards it for storage
   * in the cache.
   * @param key Query hash value to use as a storage key
   * @param data The raw data to store in RAM and/or disk
   * @param path Path to a file where the data should be persisted
   * @param fileOnly Set this to true if the data should NOT be stored in RAM
   *          and should always be pulled from disk
   * @param expire How long, in seconds, the data should remain in the cache
   *          before expiring
   * @return True if we were able to store the data successfully, false if not
   */
  public boolean storeCache(final int key, final byte[] data,
      final String path, final boolean fileOnly, final int expire) {
    // new cache entry object
    HttpCacheEntry entry = new HttpCacheEntry(key, data, path, fileOnly, expire);
    return storeCache(entry);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.cache", cache_ram_hit, "type=memory");
    collector.record("http.cache", cache_disk_hit, "type=disk");
    collector.record("http.cache", cache_miss, "cache=miss");
    collector.record("http.cache.size", cache_ram_size, "cache=memory");
    //collector.record("http.cache.size", cache.size(), "cache=objects");
    collector.record("http.cache.size", 0, "cache=disk");
  }

  /**
   * Handles various HTTP commands related to the /cache endpoint such as
   * returning cache metadata or flushing the cache Note: responses are not
   * cached, naturally
   * @param tsdb not really used in this case
   * @param query HTTP query to respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // respond with metadata about each cache object in a JSON format
    final String jsonp = JSON_HTTP.getJsonPFunction(query);
    final String file = query.hasQueryStringParam("file") ? query
        .getQueryStringParam("file") : "";

    if (!file.isEmpty()) {
      // get the cache directory
      String basepath = tsd.getConfig().cacheDirectory();
      if (System.getProperty("os.name").contains("Windows")) {
        if (!basepath.endsWith("\\"))
          basepath += "\\";
      } else {
        if (!basepath.endsWith("/"))
          basepath += "/";
      }

      // see if the file exists
      if (new File(basepath + file).exists()) {
        // TODO lookup the cache entry to get the actual cache time
        try {
          query.sendFile(basepath + file, 30);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        query.sendReply(HttpResponseStatus.NOT_FOUND, "File [" + file
            + "] not found");
      }
    } else if (query.hasQueryStringParam("flush")) {
      // TODO flush the cache
      query.sendReply(new JsonRpcError("Not implemented", 404).getJSON());
    } else {
      // the default is to print info about the cache
      final JSON_HTTP response = new JSON_HTTP(cache);
      query.sendReply(jsonp.isEmpty() ? response.getJsonString() : response
          .getJsonPString(jsonp));
    }
  }
  

  /**
   * Removes an object from the cache and attempts to delete the file associated
   * with it
   * @param key Query hash value to delete
   * @return True if the deletion was successful, false if there was an error
   */
  private boolean deleteCache(final int key) {
    HttpCacheEntry entry = cache.get(key);
    if (entry == null) {
      LOG.error("Key [" + key + "] does not exist in hash");
      return false;
    }

    // delete file if it exists
    if (!entry.getFile().isEmpty()) {
      File file = new File(entry.getFile());
      if (file.exists()) {
        if (!file.delete()) {
          LOG.error("Unable to delete file [" + entry.getFile() + "]");
          // continue on anyway so we free up RAM
        }
      }
    }

    // delete from the hash now
    if (cache.remove(key) == null) {
      LOG.error("Error removing key [" + key + "] from cache");
      return false;
    }
    LOG.debug("Removed [" + key + "] from cache");
    return true;
  }

  /**
   * Sorts the cache entries by how many hits they have received and how much
   * data is stored in RAM. Less popular entries will be ejected from RAM so
   * that we can try to store a new value.
   * @param size The amount of space we need to free
   * @return True if we could find enough room, false if there was an error
   */
  private boolean freeRAM(final int size) {
    ArrayList<HttpCacheEntryMeta> meta_list = new ArrayList<HttpCacheEntryMeta>();

    // build an ordered list of hash meta data objects
    for (Map.Entry<Integer, HttpCacheEntry> entry : cache.entrySet()) {
      meta_list.add(entry.getValue().getMeta());
    }

    // data check
    if (meta_list.size() < 1) {
      LOG.error("Meta list is empty, unable to sort or fix cache memory");
      return false;
    }

    // sort it!
    Collections.sort(meta_list);

    HttpCacheEntryMeta meta;
    Iterator<HttpCacheEntryMeta> iterator = meta_list.iterator();
    while (iterator.hasNext()
        && (cache_ram_size.longValue() + size) > ram_size_limit) {
      meta = iterator.next();
      if (meta.getDataSize() > 0) {
        LOG.debug("Flushing [" + meta.getDataSize() + "] bytes from cache ID ["
            + meta.getID() + "=");
        if (!cache.containsKey(meta.getID())) {
          LOG.error("Cache does not contain key [" + meta.getID() + "]");
          continue;
        }
        cache.get(meta.getID()).flushData();
        cache_ram_size.addAndGet(-meta.getDataSize());
      }

      if ((cache_ram_size.longValue() + size) <= ram_size_limit) {
        return true;
      }
    }
    LOG.error("Unable to free any RAM from the cache");
    return false;
  }
}

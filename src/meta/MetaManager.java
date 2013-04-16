// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles storing new TSUID and UID meta data objects when created. When meta
 * tracking is enabled, every data point coming through will update the tsuid
 * map with the latest time. That map is then flushed to storage periodically 
 * since flushing on every write would be much too intensive.
 * <p>
 * When a TSD starts up with meta tracking enabled, each TSUID will be written
 * to the {@code new_tsuids} queue. TSUIDs that already have meta data will be
 * discarded but if a new TSUID is found, new meta data is written. This means
 * a newly started TSD will use a lot of memory as the queue grows but it will
 * shrink as the worker threads work through it. The alternative is to pre-seed
 * the {@code tsuid} map on startup before accepting data, but that can cause
 * a long delay.
 * <p>
 * Every new TSUID, updates to the last received time and new UIDs are also
 * sent to the search plugin if configured.
 * <p>
 * <b>Warning:</b> On a busy TSD with many unique timeseries, the tsuid map can 
 * grow pretty big so be sure to account for that when setting heap sizes.
 * @since 2.0
 */
public final class MetaManager {
  private static final Logger LOG = LoggerFactory.getLogger(MetaManager.class);
  
  /** The TSDB to which we belong */
  private final TSDB tsdb;
  
  /** Map of timeseries UIDs written to this TSD along with the last timestamp */
  private final ConcurrentHashMap<String, Long> tsuids;
  
  /** Queue of new TSUIDs that may need meta data entries */
  private final ConcurrentLinkedQueue<String> new_tsuids;
  
  /** Queue of new UID meta objects, populated whenever a new UID is assigned */
  private final ConcurrentLinkedQueue<UIDMeta> new_uids;
  
  /** Array of worker threads to process the {@code new_tsuids} queue */
  private final Thread[] new_tsuid_processors;
  
  /** Thread to work through the {@code new_uids} queue */
  private final Thread new_uid_processor;
  
  /** How long, in ms, to wait before popping a TSUID from the 
   * {@code tsuids} map */
  private final long tsuid_expiration;
  
  /**
   * Constructor that initializes the map, queue and processing threads
   * @param tsdb The TSDB to which we belong
   */
  public MetaManager(final TSDB tsdb) {
    this.tsdb = tsdb;
    tsuids = new ConcurrentHashMap<String, Long>();
    new_tsuids = new ConcurrentLinkedQueue<String>();
    new_uids = new ConcurrentLinkedQueue<UIDMeta>();
    tsuid_expiration = 3600000;
    final int thread_count = 2;
    new_tsuid_processors = new NewTSUIDProcessor[thread_count];
    for (int i = 0; i < thread_count; i++) {
      new_tsuid_processors[i] = new NewTSUIDProcessor();
      new_tsuid_processors[i].run();
    }
    new_uid_processor = new NewUIDProcessor();
    new_uid_processor.run();
  }
  
  /**
   * Processes a TSUID.
   * If the TSUID does not exist in the map, it's added to the new tsuid queue.
   * If it does exist, then the timestamp is updated if it's newer than the
   * last received value.
   * @param tsuid The hexadecimal encoded TSUID
   * @param timestamp The latest timestamp in seconds or milliseconds
   */
  public final void queueTSUID(final String tsuid, final long timestamp) {
    final Long last_received = tsuids.get(tsuid);
    if (last_received == null) {
      new_tsuids.add(tsuid);
      tsuids.put(tsuid, timestamp);
    } else {
      if (last_received < timestamp) {
        tsuids.put(tsuid, timestamp);
      }
    }
  }
  
  /**
   * Adds a UIDMeta object to the queue for future processing
   * @param meta The meta data to add
   */
  public final void queueUIDMeta(final UIDMeta meta) {
    new_uids.add(meta);
  }
  
  /**
   * Thread that handles the {@code new_tsuids} queue. It will check to see if 
   * the tsuid has meta data in storage and if it does, then we ignore the tsuid.
   * If storage doesn't have anything, we'll create a new meta data object and
   * store it. Since new TSUIDs can come in pretty frequently, particularly on
   * TSD startup, this is meant to be one of multiple worker threads.
   */
  final class NewTSUIDProcessor extends Thread {
    
    public NewTSUIDProcessor() {
      super("NewTSUIDProcessor");
    }
    
    public void run() {
      while (true) {
        
        final String tsuid = new_tsuids.poll();
        if (tsuid == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException i) {
            LOG.error("New TSUID thread interrupted. Quiting");
            return;
          }
        }
        
        try {
          // if the meta already exists, bail so we don't waste time
          if (TSMeta.metaExistsInStorage(tsdb, tsuid)) {
            continue;
          }
          final TSMeta meta = new TSMeta(UniqueId.stringToUid(tsuid), 
              tsuids.get(tsuid));
          meta.syncToStorage(tsdb, false);
          // todo (cl) - push to search plugin
          LOG.trace("Stored new TSUID: " + meta.toString());
        } catch (Exception e) {
          LOG.error("Failed to sync new TSMeta: " + tsuid, e);
        }
      }
    }
  }
  
  /**
   * Thread that handles the {@code new_uids} queue. Any new UID passed in will
   * be written to storage. Since UID creation is usually infrequent, we don't
   * need multiple workers.
   */
  final class NewUIDProcessor extends Thread {
    public NewUIDProcessor() {
      super("NewUIDProcessor");
    }
    
    public void run() {
      while (true) {
        final UIDMeta meta = new_uids.poll();
        if (meta == null) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException i) {
            LOG.error("New UID thread interrupted. Quiting");
            return;
          }
        }
        
        try {
          // if the meta already exists in storage, it won't hurt to update it
          meta.syncToStorage(tsdb, false);
          // todo (cl) - push to search plugin
        } catch (Exception e) {
          LOG.error("Failed to sync new UIDMeta: " + meta, e);
        }
      }
    }
  }

  /**
   * Thread that continuously iterates through the {@code tsuids} map and either
   * flushes old entries to reclaim RAM or updates meta entries with the last
   * received value.
   */
  final class LastReceivedUpdater extends Thread {
    public LastReceivedUpdater() {
      super("LastReceivedUpdater");
    }
    
    public void run() {
      long last_run = 0;
      while (true) {
        // create the objects once and reuse
        long last_update = 0;
        TSMeta meta = null;
        
        for (Map.Entry<String, Long> entry : tsuids.entrySet()) {
          
          // convert the last_update to ms if it's not already since it depends
          // on what the user provides
          last_update = entry.getValue();
          if (last_update < 9999999999L) {
            last_update *= 1000;
          }
          
          // drop expired entries so we can free up a tiny bit of memory in
          // long running TSDs
          if (last_run - last_update > tsuid_expiration) {
            LOG.debug("Removing stale TSUID from map: " + entry.getKey());
            tsuids.remove(entry.getKey());
          }
          
          // if the value hasn't been updated in a while, bail
          if (last_update <= last_run) {
            continue;
          }
          
          try {
            // sync
            meta = TSMeta.getTSMeta(tsdb, entry.getKey());
            if (meta == null) {
              meta = new TSMeta(UniqueId.stringToUid(entry.getKey()), 
                  tsuids.get(entry.getKey()));
              meta.syncToStorage(tsdb, false);
              // todo (cl) - push to search plugin
              LOG.trace("Stored new TSUID: " + meta.toString());
            } else {
              if (last_update / 1000 > meta.getLastReceived()) {
                meta.setLastReceived(last_update / 1000);
                meta.syncToStorage(tsdb, false);
              }
            }
          } catch (Exception e) {
            LOG.error("Failed to update lastReceived for TSMeta: " + 
                entry.getKey(), e);
          }
        }
        
        last_run = System.currentTimeMillis();
        LOG.trace("Completed TSUID last received update run");
        
        // sleep a little so we can exit gracefully
        try {
          Thread.sleep(1000);
        } catch (InterruptedException i) {
          LOG.error("New LastReceivedUpdater thread interrupted. Quiting");
          return;
        }
      }
    }
  }
}

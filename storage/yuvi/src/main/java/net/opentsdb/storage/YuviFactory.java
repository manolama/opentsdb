// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.pinterest.yuvi.chunk.ChunkManager;
import com.pinterest.yuvi.chunk.OffHeapChunkManagerTask;
import com.pinterest.yuvi.writer.FileMetricWriter;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesId;

public class YuviFactory implements TimeSeriesDataStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(YuviFactory.class);
  
  /** A TSD to pull config data from. */
  private TSDB tsdb;
  
  private ChunkManager manager;
  OffHeapChunkManagerTask offHeapChunkManagerTask;
  ScheduledExecutorService offHeapChunkManagerScheduler;

  protected volatile YuviDataStore default_client;
  
  /** A map of non-default clients. */
  protected Map<String, YuviDataStore> clients = Maps.newConcurrentMap();
  
  @Override
  public String id() {
    return "Yuvi";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    manager = new ChunkManager("OpenTSDB", 10485760);
    if (!tsdb.getConfig().hasProperty("yuvi.datafile")) {
      tsdb.getConfig().register("yuvi.datafile", null, false, "The path to a YUVI data file to bootstrap with.");
    }
    
    if (!Strings.isNullOrEmpty(tsdb.getConfig().getString("yuvi.datafile"))) {
      LOG.info("Reading file: " + tsdb.getConfig().getString("yuvi.datafile"));
      Path filePath = Paths.get(tsdb.getConfig().getString("yuvi.datafile"));
      //Path filePath = Paths.get("Users/clarsen/Downloads/tc.stat.put_12h_sorted");
      FileMetricWriter metricWriter = new FileMetricWriter(filePath, manager);
      metricWriter.start();
      offHeapChunkManagerScheduler = null;
      offHeapChunkManagerTask = null;
    } else {
      offHeapChunkManagerScheduler =
          Executors.newScheduledThreadPool(1);
      
      offHeapChunkManagerTask =
          new OffHeapChunkManagerTask(manager,
              OffHeapChunkManagerTask.DEFAULT_METRICS_DELAY_SECS,
              12 * 3600);
      
      offHeapChunkManagerScheduler.scheduleAtFixedRate(offHeapChunkManagerTask,
          60,
          60, TimeUnit.MINUTES);
    }
    
    LOG.info("Finished initializing Yuvi store.");
    return Deferred.fromResult(null);
  }

  ChunkManager manager() {
    return manager;
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public TimeSeriesDataStore newInstance(TSDB tsdb, String id) {
    if (Strings.isNullOrEmpty(id)) {
      if (default_client == null) {      
        synchronized (this) {
          if (default_client == null) {
            default_client = new YuviDataStore(this, tsdb, id);
          }
        }
      }
      
      return default_client;
    }
    
    YuviDataStore client = clients.get(id);
    if (client == null) {
      synchronized (this) {
        client = clients.get(id);
        if (client == null) {
          client = new YuviDataStore(this, tsdb, id);
          clients.put(id, client);
        }
      }
    }
    return client;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

}

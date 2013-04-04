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
package net.opentsdb.storage.hbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hbase.async.HBaseException;
import org.hbase.async.TableNotFoundException;

import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.Tree;
import net.opentsdb.meta.TreeBranch;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.DataStore;
import net.opentsdb.storage.StorageException;
import net.opentsdb.storage.StorageQuery;
import net.opentsdb.uid.UniqueId.UIDType;
import net.opentsdb.utils.Config;

import com.stumbleupon.async.Deferred;

/**
 * Default storage engine for OpenTSDB using HBase. Completely compatible with 
 * 1.x.
 * 
 * Note this class will likely move into a plugin. We're here for now just to
 * get started
 */
public final class HBaseStorage extends DataStore {

  private static final String METRICS_QUAL = "metrics";
  private static final short METRICS_WIDTH = 3;
  private static final String TAG_NAME_QUAL = "tagk";
  private static final short TAG_NAME_WIDTH = 3;
  private static final String TAG_VALUE_QUAL = "tagv";
  private static final short TAG_VALUE_WIDTH = 3;
  
  /** Handles connections to the 'tsdb' table for fetching/storing data */
  private HBaseStorageClient data_store;
  
  /** Handles connections to the 'tsdb-uid' table for meta info */
  private HBaseStorageClient uid_store;
  
  // todo - move
  //private CompactionQueue compactionq;

  /** Unique ID cache for the metric names. */
  private UniqueId metrics;
  
  /** Unique ID cache for the tag names. */
  private UniqueId tag_names;
  
  /** Unique ID cache for the tag values. */
  private UniqueId tag_values;
  
  /**
   * Initializes the storage backend and makes sure the data and UID tables
   * exist. If they don't this will toss an exception
   * @throws StorageException if the tables don't exist or there was an issue
   * connecting to the cluster
   */
  @Override
  public final void initialize(Config config) {
    this.data_store = new HBaseStorageClient(config,
        config.getString("tsd.storage.hbase.data_table"));
    this.uid_store = new HBaseStorageClient(config,
        config.getString("tsd.storage.hbase.uid_table"));
    this.metrics = new UniqueId(this.uid_store, METRICS_QUAL, METRICS_WIDTH);
    this.tag_names = new UniqueId(this.uid_store, TAG_NAME_QUAL, TAG_NAME_WIDTH);
    this.tag_values = new UniqueId(this.uid_store, TAG_VALUE_QUAL, 
        TAG_VALUE_WIDTH);
    
    try {
      this.data_store.ensureTableExists();
      this.uid_store.ensureTableExists();
    } catch (TableNotFoundException tnfe) {
      throw new StorageException(tnfe.getMessage(), tnfe);
    } catch (HBaseException hbe) {
      throw new StorageException(hbe.getMessage(), hbe);
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public final Deferred<ArrayList<Object>> shutdown() {
    return Deferred.group(this.data_store.shutdown(), 
        this.uid_store.shutdown());
  }

  @Override
  public final Deferred<ArrayList<Object>> flushData() {
    return Deferred.group(this.data_store.flush(), this.uid_store.flush());
  }

  @Override
  public final void collectStats(StatsCollector collector) {
    // TODO Auto-generated method stub 
  }

  @Override
  public final Deferred<String> getName(UIDType type, byte[] uid) {
    try {
      switch (type) {
        case METRIC:
          return Deferred.fromResult(this.metrics.getName(uid));
        case TAGK:
          return Deferred.fromResult(this.tag_names.getName(uid));
        case TAGV:
          return Deferred.fromResult(this.tag_values.getName(uid));
        default:
          throw new StorageException("Invalid UID type");
      }
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public final Deferred<byte[]> getUID(UIDType type, String name) {
    try {
      switch (type) {
        case METRIC:
          return Deferred.fromResult(this.metrics.getId(name));
        case TAGK:
          return Deferred.fromResult(this.tag_names.getId(name));
        case TAGV:
          return Deferred.fromResult(this.tag_values.getId(name));
        default:
          throw new StorageException("Invalid UID type");
      }
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public final Deferred<byte[]> getOrAssignUID(UIDType type, String name) {
    try {
      switch (type) {
        case METRIC:
          return Deferred.fromResult(this.metrics.getOrCreateId(name));
        case TAGK:
          return Deferred.fromResult(this.tag_names.getOrCreateId(name));
        case TAGV:
          return Deferred.fromResult(this.tag_values.getOrCreateId(name));
        default:
          throw new StorageException("Invalid UID type");
      }
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public final Deferred<Object> renameUID(UIDType type, String current_name,
      String new_name) {
    try {
      switch (type) {
        case METRIC:
          this.metrics.rename(current_name, new_name);
          break;
        case TAGK:
          this.tag_names.rename(current_name, new_name);
          break;
        case TAGV:
          this.tag_values.rename(current_name, new_name);
          break;
        default:
          throw new StorageException("Invalid UID type");
      }
      return Deferred.fromResult(null);
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }

  @Override
  public final Deferred<ArrayList<String>> suggestUIDs(final UIDType type, 
      final String search, final int count) {
    try {
      switch (type) {
        case METRIC:
          return Deferred.fromResult(this.metrics.suggest(search, count));
        case TAGK:
          return Deferred.fromResult(this.tag_names.suggest(search, count));
        case TAGV:
          return Deferred.fromResult(this.tag_values.suggest(search, count));
        default:
          throw new StorageException("Invalid UID type");
      }
    } catch (Exception e) {
      throw new StorageException(e.getMessage(), e);
    }
  }
  
  @Override
  public Deferred<Object> fsckUIDs(boolean fix, StorageQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Map<String, byte[]>> grepUIDs(UIDType type, String regex_query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteUIDs(UIDType type, String[] uids,
      StorageQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteTSUIDs(String[] tsuids, StorageQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<TreeMap<byte[], Span>> queryTSData(TsdbQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteTSData(TsdbQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> fsckTSData(boolean fix, TsdbQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Boolean> putDataPoint(String metric, long timestamp,
      Object value, Map<String, String> tags) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Annotation> getAnnotation(long start, String tsuid) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Boolean> deleteAnnotations(Map<Long, String> annotations) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<Annotation>> getAnnotations(long start, long end,
      String tsuid, StorageQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Annotation> putAnnotation(Annotation note) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<UIDMeta> getUIDMetaData(UIDType type, byte[] uid) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<UIDMeta> putUIDMetaData(UIDMeta meta) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<TSMeta> getTSMetaData(byte[] tsuid) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<HashSet<Integer>> getAllTSUIDs() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<TSMeta>> scanTSUIDs(StorageQuery query) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<TreeBranch> getBranch(int tree_id, int branch_id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Boolean> deleteBranch(int tree_id, int branch_id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<TreeBranch> putBranch(TreeBranch branch) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Tree> getTree(int tree_id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Boolean> deleteTree(int tree_id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Tree> putTree(Tree tree) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Tree> getNewTree() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<ArrayList<Object>> flushCaches() {
    this.metrics.dropCaches();
    this.tag_names.dropCaches();
    this.tag_values.dropCaches();
    return Deferred.fromResult(null);
  }

// PRIVATE methods 
  
}

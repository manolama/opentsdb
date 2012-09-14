// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaData;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.TsdbStoreHBase;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public final class TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
  
  static final byte[] FAMILY = { 't' };

  private static final String METRICS_QUAL = "metrics";
  private static final short METRICS_WIDTH = 3;
  private static final String TAG_NAME_QUAL = "tagk";
  private static final short TAG_NAME_WIDTH = 3;
  private static final String TAG_VALUE_QUAL = "tagv";
  private static final short TAG_VALUE_WIDTH = 3;

  static final boolean enable_compactions;
  static {
    final String compactions = System.getProperty("tsd.feature.compactions");
    enable_compactions = compactions != null && !"false".equals(compactions);
  }

  /** Client for the HBase cluster to use.  */
  final TsdbStore storage;

  /** Configuration for the TSD and related services */
  final Config config;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  
  public volatile Set<String> ts_uids = new TreeSet<String>();

  /** Unique IDs for the metric names. */
  public final UniqueId metrics;
  /** Unique IDs for the tag names. */
  public final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  public final UniqueId tag_values;

  private final MetaData timeseries_meta;
  /** Thread that synchronizes UID maps */
  private volatile UIDManager uid_manager;
  
  /**
   * Row keys that need to be compacted.
   * Whenever we write a new data point to a row, we add the row key to this
   * set.  Every once in a while, the compaction thread will go through old
   * row keys and will read re-compact them.
   */
  private final CompactionQueue compactionq;

  /**
   * DEPRECATED Constructor
   * Please use the constructor with the Config class instead
   * @param client The HBase client to use.
   * @param timeseries_table The name of the HBase table where time series
   * data is stored.
   * @param uniqueids_table The name of the HBase table where the unique IDs
   * are stored.
   */
  public TSDB(final HBaseClient client,
              final String timeseries_table,
              final String uniqueids_table) {
    //this.client = client;
    this.config = new Config();
    table = timeseries_table.getBytes();
    this.config.tsdTable(timeseries_table);
    this.config.tsdUIDTable(uniqueids_table);
    this.storage = new TsdbStoreHBase(table, client);
    
    final byte[] uidtable = uniqueids_table.getBytes();
    metrics = new UniqueId(client, uidtable, METRICS_QUAL, METRICS_WIDTH);
    tag_names = new UniqueId(client, uidtable, TAG_NAME_QUAL, TAG_NAME_WIDTH);
    tag_values = new UniqueId(client, uidtable, TAG_VALUE_QUAL,
                              TAG_VALUE_WIDTH);
    compactionq = new CompactionQueue(this);
    timeseries_meta = new MetaData(client, uidtable, true, "name");
  }
  
  /**
   * Constructor.
   * @param client The HBase client to use.
   * @param timeseries_table The name of the HBase table where time series
   * data is stored.
   * @param uniqueids_table The name of the HBase table where the unique IDs
   * are stored.
   */
  public TSDB(final HBaseClient client,
              final Config config) {
    //this.client = client;
    this.config = config;
    table = config.tsdTable().getBytes();
    this.storage = new TsdbStoreHBase(table, client);
    
    final byte[] uidtable = config.tsdUIDTable().getBytes();
    metrics = new UniqueId(client, uidtable, METRICS_QUAL, METRICS_WIDTH);
    tag_names = new UniqueId(client, uidtable, TAG_NAME_QUAL, TAG_NAME_WIDTH);
    tag_values = new UniqueId(client, uidtable, TAG_VALUE_QUAL,
                              TAG_VALUE_WIDTH);
    compactionq = new CompactionQueue(this);
    timeseries_meta = new MetaData(client, uidtable, true, "name");
  }

  /**
   * Initializes management objects and starts threads. Should only be called
   * if this is running a full TSDB instance. Don't call this if you're writing
   * utilities.
   */
  public void startManagementThreads(){
    uid_manager = new UIDManager(config.tsdUIDTable());
    uid_manager.start();
  }
  
  /** Number of cache hits during lookups involving UIDs. */
  public int uidCacheHits() {
    return (metrics.cacheHits() + tag_names.cacheHits()
            + tag_values.cacheHits());
  }

  /** Number of cache misses during lookups involving UIDs. */
  public int uidCacheMisses() {
    return (metrics.cacheMisses() + tag_names.cacheMisses()
            + tag_values.cacheMisses());
  }

  /** Number of cache entries currently in RAM for lookups involving UIDs. */
  public int uidCacheSize() {
    return (metrics.cacheSize() + tag_names.cacheSize()
            + tag_values.cacheSize());
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public void collectStats(final StatsCollector collector) {
    collectUidStats(metrics, collector);
    collectUidStats(tag_names, collector);
    collectUidStats(tag_values, collector);

    {
      final Runtime runtime = Runtime.getRuntime();
      collector.record("jvm.ramfree", runtime.freeMemory());
      collector.record("jvm.ramused", runtime.totalMemory());
    }

    collector.addExtraTag("class", "IncomingDataPoints");
    try {
      collector.record("hbase.latency", IncomingDataPoints.putlatency, "method=put");
    } finally {
      collector.clearExtraTag("class");
    }

    collector.addExtraTag("class", "TsdbQuery");
    try {
      collector.record("hbase.latency", TsdbQuery.scanlatency, "method=scan");
    } finally {
      collector.clearExtraTag("class");
    }
//    collector.record("hbase.root_lookups", client.rootLookupCount());
//    collector.record("hbase.meta_lookups",
//                     client.uncontendedMetaLookupCount(), "type=uncontended");
//    collector.record("hbase.meta_lookups",
//                     client.contendedMetaLookupCount(), "type=contended");

    compactionq.collectStats(collector);
  }

  /** Returns a latency histogram for Put RPCs used to store data points. */
  public Histogram getPutLatencyHistogram() {
    return IncomingDataPoints.putlatency;
  }

  /** Returns a latency histogram for Scan RPCs used to fetch data points.  */
  public Histogram getScanLatencyHistogram() {
    return TsdbQuery.scanlatency;
  }

  /**
   * Collects the stats for a {@link UniqueId}.
   * @param uid The instance from which to collect stats.
   * @param collector The collector to use.
   */
  private static void collectUidStats(final UniqueId uid,
                                      final StatsCollector collector) {
    collector.record("uid.cache-hit", uid.cacheHits(), "kind=" + uid.kind());
    collector.record("uid.cache-miss", uid.cacheMisses(), "kind=" + uid.kind());
    collector.record("uid.cache-size", uid.cacheSize(), "kind=" + uid.kind());
  }

  /**
   * Returns a new {@link Query} instance suitable for this TSDB.
   */
  public Query newQuery() {
    return new TsdbQuery(this);
  }

  /**
   * Returns a new {@link WritableDataPoints} instance suitable for this TSDB.
   * <p>
   * If you want to add a single data-point, consider using {@link #addPoint}
   * instead.
   */
  public WritableDataPoints newDataPoints() {
    return new IncomingDataPoints(this);
  }

  /**
   * Adds a single integer value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final long value,
                                   final Map<String, String> tags) {
    final short flags = 0x7;  // An int stored on 8 bytes.
    return addPointInternal(metric, timestamp, Bytes.fromLong(value),
                            tags, flags);
  }

  /**
   * Adds a single floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final float value,
                                   final Map<String, String> tags) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(metric, timestamp,
                            Bytes.fromInt(Float.floatToRawIntBits(value)),
                            tags, flags);
  }

  private Deferred<Object> addPointInternal(final String metric,
                                            final long timestamp,
                                            final byte[] value,
                                            final Map<String, String> tags,
                                            final short flags) {
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      // => timestamp < 0 || timestamp > Integer.MAX_VALUE
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to add value=" + Arrays.toString(value) + '/' + flags
          + " to metric=" + metric + ", tags=" + tags);
    }

    IncomingDataPoints.checkMetricAndTags(metric, tags);
    final byte[] row = IncomingDataPoints.rowKeyTemplate(this, metric, tags);
    final long base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    Bytes.setInt(row, (int) base_time, metrics.width());
    scheduleForCompaction(row, (int) base_time);
    final short qualifier = (short) ((timestamp - base_time) << Const.FLAG_BITS
                                     | flags);
//    final PutRequest point = new PutRequest(table, row, FAMILY,
//                                            Bytes.fromShort(qualifier), value);
//    // TODO(tsuna): Add a callback to time the latency of HBase and store the
//    // timing in a moving Histogram (once we have a class for this).
//    return client.put(point);
    return storage.putWithRetry(row, FAMILY, Bytes.fromShort(qualifier), value);
  }

  /**
   * Forces a flush of any un-committed in memory data.
   * <p>
   * For instance, any data point not persisted will be sent to HBase.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored.  The value of the deferred
   * object return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> flush() throws HBaseException {
    LOG.trace("Flushing all objects to storage");
    // force sync of the timestamp uids
    uid_manager.interrupt();
    uid_manager = null;
    LOG.trace("Flushing TS UIDs");
    syncTSUIDs();
    
    LOG.trace("Flushing metric maps");
    this.metrics.flushMaps(true);
    
    LOG.trace("Flushing tagk maps");
    this.tag_names.flushMaps(true);
    
    LOG.trace("Flushing tagv maps");
    this.tag_values.flushMaps(true);
    
    return storage.flush();
  }

  /**
   * Gracefully shuts down this instance.
   * <p>
   * This does the same thing as {@link #flush} and also releases all other
   * resources.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored, and all resources used by
   * this instance have been released.  The value of the deferred object
   * return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> shutdown() {
    final class HClientShutdown implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> args) {
        return storage.shutdown();
      }
      public String toString() {
        return "shutdown HBase client";
      }
    }
    // First flush the compaction queue, then shutdown the HBase client.
    return enable_compactions
      ? compactionq.flush().addBoth(new HClientShutdown())
      : storage.shutdown();
  }

  /**
   * Fetches the entire cache of Metrics
   * @return A sorted list of metrics in HBase
   */
  public final SortedMap<String, Long> getMetrics(){
    return metrics.getMap();
  }
  
  /**
   * Fetches the entire cache of tag names
   * @return A sorted list of tag names
   */
  public final SortedMap<String, Long> getTagNames(){
    return this.tag_names.getMap();
  }
  
  /**
   * Fetches the entire cache of tag values
   * @return A sorted list of tag values
   */
  public final SortedMap<String, Long> getTagValues(){
    return this.tag_values.getMap();
  }
  
  /**
   * Given a prefix search, returns a few matching metric names.
   * @param search A prefix to search.
   */
  public List<String> suggestMetrics(final String search) {
    return metrics.suggest(search);
  }

  /**
   * Given a prefix search, returns a few matching tag names.
   * @param search A prefix to search.
   */
  public List<String> suggestTagNames(final String search) {
    return tag_names.suggest(search);
  }

  /**
   * Given a prefix search, returns a few matching tag values.
   * @param search A prefix to search.
   */
  public List<String> suggestTagValues(final String search) {
    return tag_values.suggest(search);
  }

  /**
   * Returns the configuration reference
   * @return Config reference
   */
  public Config getConfig(){
    return this.config;
  }
  
  public TimeSeriesMeta getTimeSeriesMeta(final byte[] id){
    TimeSeriesMeta meta = this.timeseries_meta.getTimeSeriesMeta(id);
    if (meta == null)
      return new TimeSeriesMeta(id);
    
    // otherwise we need to get the general metas for metrics and tags
    byte[] metricID = MetaData.getMetricID(id);
    if (metricID == null)
      LOG.debug(String.format("Unable to get metric meta data for ID [%s]", 
          UniqueId.IDtoString(id)));
    else
      meta.setMetric(this.metrics.getGeneralMeta(metricID));
    
    // tags
    ArrayList<byte[]> tags = MetaData.getTagIDs(id);
    if (tags == null || tags.size() < 1)
      LOG.debug(String.format("Unable to get tag and value metadata for ID [%s]",
          UniqueId.IDtoString(id)));
    else{
      ArrayList<GeneralMeta> tm = new ArrayList<GeneralMeta>();
      int index=0;
      for (byte[] tag : tags){
        if ((index % 2) == 0)
          tm.add(this.tag_names.getGeneralMeta(tag));
        else
          tm.add(this.tag_values.getGeneralMeta(tag));
        index++;
      }
      meta.setTags(tm);
    }
    
    return meta;
  }
  
  public Boolean putMeta(final TimeSeriesMeta meta){
    return this.timeseries_meta.putMeta(meta);
  }
  
  /**
   * Attempts to synchronize the local Timeseries UID with the one in storage
   * 
   * First, the method locks the row the timeseries UID is in, fetches it
   * and checks to see if there are any differences between the local and the
   * stored array. If there is, then we merge the two and write the changes
   * back to storage.
   * @return True if the sync was successful, false if not.
   */
  @SuppressWarnings("unchecked")
  public synchronized final Boolean syncTSUIDs(){
    final TsdbStore local_store = storage;
    local_store.setTable(config.tsdUIDTable());

    short attempt = 3;
    Object lock = null;
    byte[] uid_row = new byte[] { 0 };
    try{
      while(attempt-- > 0){
        LOG.debug(String.format("Attempting to sync Timestamp UIDs tid [%d]", 
            Thread.currentThread().getId()));
        // first, we need to lock the row for exclusive access on the set
        try {
          lock = local_store.getRowLock(uid_row);          
          if (lock == null) {  // Should not happen.
            LOG.error("Received null for row lock");
            continue;
          }
          LOG.debug(String.format("Successfully locked UID row [%s]", UniqueId.IDtoString(uid_row)));
          
          Set<String> temp_uids = new TreeSet<String>();
          JSON codec = new JSON(temp_uids);
          
          // get the current value from storage so we don't overwrite other TSDs changes
          byte[] uids = local_store.getValue(new byte[] {0}, TsdbStore.toBytes("id"), 
              TsdbStore.toBytes("ts_uids"), lock);
          if (uids == null){
            LOG.warn("Timeseries UID list was not found in the storage system");
          }else{
            if (!codec.parseObject(uids)){
              LOG.error("Unable to parse Timeseries UID list from the storage system");
              return false;
            }
            temp_uids = (TreeSet<String>)codec.getObject();
            if (temp_uids.size() > 0)
              LOG.debug(String.format("Successfully loaded Timeseries UID list from the storage system [%d] tsuids",
                temp_uids.size()));
            
            // if we've just loaded the TSDB, we don't need to bother with comparissons
            if (ts_uids.size() < 1){
              ts_uids = temp_uids;
              return true;
            }
            
            // now we compare the newly loaded list and the old one, if there are any differences,
            // we need to update storage
            if (ts_uids.equals(temp_uids)){
              LOG.debug("No changes from stored data");
              return true;
            }
          }          
          
          // there was a difference so merge the two sets, then write to storage
          int old_size = ts_uids.size();
          ts_uids.addAll(temp_uids);
          if (ts_uids.size() < 1){
            LOG.debug("No UIDs to store");
            return true;
          }
          
          LOG.trace(String.format("TS UIDs requires updating, old size [%d], new [%d]",
              old_size, ts_uids.size()));            
          
          codec = new JSON(ts_uids);
          local_store.putWithRetry(uid_row, TsdbStore.toBytes("id"), 
              TsdbStore.toBytes("ts_uids"), codec.getJsonBytes(), lock)
              .joinUninterruptibly();
          LOG.info("Successfully updated Timeseries UIDs in storage");
          // do NOT forget to unlock
          LOG.trace("Releasing lock");
          local_store.releaseRowLock(lock);
        } catch (TsdbStorageException e) {
          try {
            Thread.sleep(61000 / 3);
          } catch (InterruptedException ie) {
            return false;
          }
          continue;
        } catch (Exception e){
          LOG.error(String.format("Unhandled exception [%s]", e));
          e.printStackTrace();
          return false;
        }
      }
    }catch (TsdbStorageException tex){
      LOG.warn(String.format("Exception from storage [%s]", tex.getMessage()));
      return false;
    } finally {
      LOG.trace("Releasing lock");
      local_store.releaseRowLock(lock);
    }
    return true;
  }
  
  /**
   * Updates the UID maps and meta data when a new timeseries UID is detected
   * @param row_key Timeseries UID to process
   * @return True if updates were successful, false if there was an error
   */
  public synchronized final Boolean processNewTSUID(final byte[] row_key){
    
    // update maps
    String metric = UniqueId.IDtoString(UniqueId.getMetricFromKey(row_key, (short)3));
    List<byte[]> pairs = UniqueId.getTagPairsFromKey(row_key, (short)3, (short)3, (short)4);
    List<byte[]> tagks = UniqueId.getTagksFromTagPairs(pairs, (short)3);
    List<byte[]> tagvs = UniqueId.getTagvsFromTagPairs(pairs, (short)3);
    
    // metric            
    for (byte[] p : pairs)
      metrics.putMap(metric, UniqueId.IDtoString(p), "tags");
    
    // tagk
    for (byte[] tagk : tagks){
      for (byte[] p : pairs)
        tag_names.putMap(UniqueId.IDtoString(tagk), UniqueId.IDtoString(p), "tags");
      
      // meta data
      GeneralMeta meta = this.metrics.getGeneralMeta(tagk);
      if (meta == null){
        meta = new GeneralMeta(tagk);
        meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
        this.tag_names.putMeta(meta);
      }else if (meta.getCreated() < 1){
        meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
        this.tag_names.putMeta(meta);
      }
    }
    
    // tagv
    for (byte[] tagv : tagvs){
      for (byte[] p : pairs)
        tag_values.putMap(UniqueId.IDtoString(tagv), UniqueId.IDtoString(p), "tags");
      
      // meta data
      GeneralMeta meta = this.metrics.getGeneralMeta(tagv);
      if (meta == null){
        meta = new GeneralMeta(tagv);
        meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
        this.tag_values.putMeta(meta);
      }else if (meta.getCreated() < 1){
        meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
        this.tag_values.putMeta(meta);
      }
    }
    
    // metric meta data
    GeneralMeta meta = this.metrics.getGeneralMeta(UniqueId.StringtoID(metric));
    if (meta == null){
      meta = new GeneralMeta(UniqueId.StringtoID(metric));
      meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
      this.metrics.putMeta(meta);
    }else if (meta.getCreated() < 1){
      meta.setCreated(Bytes.getUnsignedInt(row_key, (short)3));
      this.metrics.putMeta(meta);
    }
    
    // TS meta data
    String ts_uid = UniqueId.IDtoString(UniqueId.getTSUIDFromKey(row_key, (short)3, (short)4));
    TimeSeriesMeta tsmd = this.timeseries_meta.getTimeSeriesMeta(UniqueId.StringtoID(ts_uid));
    if (tsmd == null){
      tsmd = new TimeSeriesMeta(UniqueId.StringtoID(ts_uid));
      tsmd.setFirstReceived(Bytes.getUnsignedInt(row_key, (short)3));
      this.timeseries_meta.putMeta(tsmd);
    }else if (tsmd.getFirstReceived() < 1){
      tsmd.setFirstReceived(Bytes.getUnsignedInt(row_key, (short)3));
      this.timeseries_meta.putMeta(tsmd);
    }

    return true;
  }
  
  // ------------------ //
  // Compaction helpers //
  // ------------------ //

  final KeyValue compact(final ArrayList<KeyValue> row) {
    return compactionq.compact(row);
  }

  /**
   * Schedules the given row key for later re-compaction.
   * Once this row key has become "old enough", we'll read back all the data
   * points in that row, write them back to HBase in a more compact fashion,
   * and delete the individual data points.
   * @param row The row key to re-compact later.  Will not be modified.
   * @param base_time The 32-bit unsigned UNIX timestamp.
   */
  final void scheduleForCompaction(final byte[] row, final int base_time) {
    if (enable_compactions) {
      compactionq.add(row);
    }
  }

  
  /**
   * This little class will handle synchronization of the TS UIDs hash set
   *
   */
  private final class UIDManager extends Thread {

    private final TsdbStore local_store = storage;
    private long last_ts_uid_load = 0;
    
    /**
     * Constructor requires the UID table name to overload the table stored
     * in the storage client
     * @param uid_table UID table name
     */
    public UIDManager(String uid_table){
      local_store.setTable(uid_table);
    }    
    
    /**
     * Runs the thread that handles the UID tasks
     */
    public void run(){
      int last_tsuid_size = 0;

      while(true){
        
        // update the TS UIDs
        if (ts_uids.size() != last_tsuid_size || 
            ((System.currentTimeMillis() / 1000) - last_ts_uid_load) >= 15){
          LOG.trace("Triggering TS UID sync");
          syncTSUIDs();
          last_tsuid_size = ts_uids.size();
          last_ts_uid_load = System.currentTimeMillis() / 1000;
        }
        
        try {
          Thread.sleep(15000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
    
    
  }
}

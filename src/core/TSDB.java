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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Pattern;
import java.util.AbstractMap.SimpleEntry;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.TimeseriesUID;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaDataCache;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchIndexer;
import net.opentsdb.search.SearchManager;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.Searcher;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public final class TSDB {
  public enum TSDRole{
    Ingest,     /** Simply accepts incoming data */
    Forwarder,  /** Stores and forwards data to Ingesters */
    API,        /** HTTP API server, handles requests but not putting data */
    Roller,     /** Performs rollups/aggregations */
    Esper,      /** Esper alert node */
    Tool        /** CLi tool */
  }
  
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
  public final TsdbStore uid_storage;
  final TsdbStore data_storage;

  /** Configuration for the TSD and related services */
  final Config config;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  
  /** This will be used for puts */
  //public volatile Set<String> ts_uids = new TreeSet<String>();
  public final TimeseriesUID ts_uids;
  
  /** This will store just the short info like metric and tags for tsuids */
  private volatile Map<String, Map<String, Object>> tsuid_short_meta = 
    new HashMap<String, Map<String, Object>>();

  /** Unique IDs for the metric names. */
  public final UniqueId metrics;
  /** Unique IDs for the tag names. */
  public final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  public final UniqueId tag_values;

  private final MetaDataCache timeseries_meta;
  /** Thread that synchronizes UID maps */
  private UIDManager uid_manager;
  private TSUIDManager tsuid_manager;
  private SearchManager search_manager;
  
  /**
   * Row keys that need to be compacted.
   * Whenever we write a new data point to a row, we add the row key to this
   * set.  Every once in a while, the compaction thread will go through old
   * row keys and will read re-compact them.
   */
  private final CompactionQueue compactionq;
  
  public final SearchIndexer meta_search_writer;
  public final Searcher meta_searcher;
  
  public final TSDRole role;
  
  /**
   * DEPRECATED Constructor
   * Please use the constructor with the Config class instead
   * @param uid_client The HBase client to use for UID tasks
   * @param data_client The HBase client to use for data tasks
   * @param timeseries_table The name of the HBase table where time series
   * data is stored.
   * @param uniqueids_table The name of the HBase table where the unique IDs
   * are stored.
   */
  public TSDB(final TsdbStore uid_store, final TsdbStore data_store, 
      final String timeseries_table, final String uniqueids_table, final TSDRole role) {
    //this.client = client;
    this.config = new Config();
    table = timeseries_table.getBytes();
    this.config.tsdTable(timeseries_table);
    this.config.tsdUIDTable(uniqueids_table);
    this.uid_storage = uid_store;
    this.data_storage = data_store;
        
    final byte[] uidtable = uniqueids_table.getBytes();
    metrics = new UniqueId(uid_storage, uidtable, METRICS_QUAL, METRICS_WIDTH);
    tag_names = new UniqueId(uid_storage, uidtable, TAG_NAME_QUAL, TAG_NAME_WIDTH);
    tag_values = new UniqueId(uid_storage, uidtable, TAG_VALUE_QUAL,
                              TAG_VALUE_WIDTH);
    compactionq = new CompactionQueue(this);
    timeseries_meta = new MetaDataCache(uid_storage, uidtable, true, "ts");
    ts_uids = new TimeseriesUID(this.uid_storage);
    if (config.role() != role && role != TSDRole.Tool)
      this.role = config.role();
    else
      this.role = role;
    if (role == TSDRole.API){
      meta_search_writer = new SearchIndexer(config.searchIndexPath());
      meta_searcher = new Searcher(config.searchIndexPath());
    }else{
      meta_search_writer = null;
      meta_searcher = null;
    }
    
  }
  
  /**
   * Constructor.
   * @param uid_client The HBase client to use for UID tasks
   * @param data_client the HBase client to use for data tasks
   * @param timeseries_table The name of the HBase table where time series
   * data is stored.
   * @param uniqueids_table The name of the HBase table where the unique IDs
   * are stored.
   */
  public TSDB(final TsdbStore uid_store, final TsdbStore data_store, final Config config,
      final TSDRole role) {
    //this.client = client;
    this.config = config;
    table = config.tsdTable().getBytes();
    this.uid_storage = uid_store;
    this.data_storage = data_store;
    
    final byte[] uidtable = config.tsdUIDTable().getBytes();
    metrics = new UniqueId(uid_storage, uidtable, METRICS_QUAL, METRICS_WIDTH);
    tag_names = new UniqueId(uid_storage, uidtable, TAG_NAME_QUAL, TAG_NAME_WIDTH);
    tag_values = new UniqueId(uid_storage, uidtable, TAG_VALUE_QUAL,
                              TAG_VALUE_WIDTH);
    compactionq = new CompactionQueue(this);
    timeseries_meta = new MetaDataCache(uid_storage, uidtable, true, "ts");
    ts_uids = new TimeseriesUID(this.uid_storage);
    if (config.role() != role && role != TSDRole.Tool)
      this.role = config.role();
    else
      this.role = role;
    if (role == TSDRole.API){
      meta_search_writer = new SearchIndexer(config.searchIndexPath());
      meta_searcher = new Searcher(config.searchIndexPath());
    }else{
      meta_search_writer = null;
      meta_searcher = null;
    }
  }

  /**
   * Initializes management objects and starts threads. Should only be called
   * if this is running a full TSDB instance. Don't call this if you're writing
   * utilities.
   */
  public void startManagementThreads(){
    uid_manager = new UIDManager();
    uid_manager.start();
//    tsuid_manager = new TSUIDManager();
//    tsuid_manager.start();
    if (role == TSDRole.API){
      search_manager = new SearchManager(this);
      search_manager.start();
    }
  }
  
  /**
   * 
   * This data never expires so we don't need to worry about that aspect
   * @param tsuid The TSUID to lookup or fetch data for
   * @return Null if there was an error looking up any metric or tag, a map with
   * the metadata if successful
   */
  public final Map<String, Object> getTSUIDShortMeta(final String tsuid){
    Map<String, Object> meta = this.tsuid_short_meta.get(tsuid);
    if (meta != null){
      return meta;
    }
    
    if (tsuid.length() < 18){
      LOG.warn(String.format("TSUID [%s] is less than 18 characters, missing tags possibly", tsuid));
      return null;
    }
    
    //LOG.trace(String.format("Cache miss on [%s]", tsuid));
    String mid = tsuid.substring(0, 6);
    String metric = null;
    try{
      metric = metrics.getName(UniqueId.StringtoID(mid));
    } catch (NoSuchUniqueId nsui){
      LOG.trace(String.format("No metric UID for [%s] in tsuid [%s]", mid, tsuid));
      throw nsui;
    }
    
    // explode tags
    List<String> pairs = new ArrayList<String>();
    for (int i = 6; i<tsuid.length(); i+=12){
      if (i + 12 > tsuid.length()){
        LOG.warn(String.format("TSUID [%s] is of the wrong length, not the proper number of tag/value pairs", tsuid));
        return null;
      }
      pairs.add(tsuid.substring(i, i + 12));
    }
    Map<String, String> tags = new HashMap<String, String>();
    for (String pair : pairs){
      String t = "";
      String v = "";
      try{ 
        t = tag_names.getName(UniqueId.StringtoID(pair.substring(0, 6)));
      } catch (NoSuchUniqueId nsui){
        LOG.debug(String.format("No tagk UID for [%s] from tsuid [%s]",
            pair.substring(0, 6), tsuid));
        throw nsui;
      }
      try{ 
        v = tag_values.getName(UniqueId.StringtoID(pair.substring(6)));
      } catch (NoSuchUniqueId nsui){
        LOG.debug(String.format("No tagv UID for [%s] from tsuid", 
            pair.substring(6), tsuid));
        throw nsui;
      }
      tags.put(t, v);
    }
    
    Map<String, Object> v = new HashMap<String, Object>();
    v.put("metric", metric);
    v.put("uid", tsuid);
    v.put("tags", tags);
    tsuid_short_meta.put(tsuid, v);
    return v;
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
    return (metrics.cacheSizeName() + tag_names.cacheSizeName()
            + tag_values.cacheSizeName());
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public void collectStats(final StatsCollector collector) {
    collectUidStats(metrics, collector);
    collectUidStats(tag_names, collector);
    collectUidStats(tag_values, collector);
    collector.record("uid.cache.size.tsuid.hashes", this.ts_uids.hashSize());
    collector.record("uid.cache.size.tsuid.queue", this.ts_uids.queueSize());
    collector.record("uid.cache.size.tsuid.meta", this.timeseries_meta.size());
    this.ts_uids.collectStats(collector);
    IncomingDataPoints.collectStats(collector);
    {
      final Runtime runtime = Runtime.getRuntime();
      collector.record("jvm.ramfree", runtime.freeMemory());
      collector.record("jvm.ramused", runtime.totalMemory());
    }

    collector.addExtraTag("class", "IncomingDataPoints");
    try {
      collector.record("hbase.latency", IncomingDataPoints.putlatency, 
        new SimpleEntry<String, String>("method", "put"));
    } finally {
      collector.clearExtraTag("class");
    }

    collector.addExtraTag("class", "TsdbQuery");
    try {
      collector.record("hbase.latency", TsdbQuery.scanlatency, 
          new SimpleEntry<String, String>("method", "scan"));
    } finally {
      collector.clearExtraTag("class");
    }
    this.data_storage.collectStats(collector);
//    collector.record("hbase.root_lookups", client.rootLookupCount());
//    collector.record("hbase.meta_lookups",
//                     client.uncontendedMetaLookupCount(), "type=uncontended");
//    collector.record("hbase.meta_lookups",
//                     client.contendedMetaLookupCount(), "type=contended");

    compactionq.collectStats(collector);
  }

  /** Returns a latency histogram for Put RPCs used to store data points. */
  public DescriptiveStatistics getPutLatencyHistogram() {
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
    collector.record("uid.cache.hits", uid.cacheHits(), 
        new SimpleEntry<String, String>("kind", uid.kind()));
    collector.record("uid.cache.miss", uid.cacheMisses(), 
        new SimpleEntry<String, String>("kind", uid.kind()));
    collector.record("uid.cache.size.name", uid.cacheSizeName(), 
        new SimpleEntry<String, String>("kind", uid.kind()));
    collector.record("uid.cache.size.id", uid.cacheSizeID(), 
        new SimpleEntry<String, String>("kind", uid.kind()));
    collector.record("uid.cache.size.meta", uid.cacheSizeMeta(), 
        new SimpleEntry<String, String>("kind", uid.kind()));
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
  
  /**
   * Attempts to determine a type for the value (integer vs float) and store it
   * @param metric
   * @param timestamp
   * @param value
   * @param tags
   * @return
   */
  public Deferred<Object> addPoint(final String metric,
        final long timestamp,
        final String value,
        final Map<String, String> tags) {
    
    try{
      if (value.toString().indexOf('.') < 0)
        return addPoint(metric, timestamp,
            Tags.parseLong(value.toString()), tags);
      else
        return addPoint(metric, timestamp, 
            Float.parseFloat(value.toString()), tags);
    }catch (NumberFormatException nfe){
      throw new IllegalArgumentException(String.format("Unable to convert metric [%s] value [%s]: %s", 
          metric, value, nfe.getMessage()));
    }catch (NullPointerException npe){
      throw new IllegalArgumentException("Value for the datapoint was null");
    }
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
    final String tsuid = UniqueId.IDtoString(TimeseriesUID.getTSUIDFromKey(row, (short)3, (short)4)).intern();
    if (!this.ts_uids.contains(tsuid)){
      this.ts_uids.add(tsuid);
    }
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
    final long start_put = System.nanoTime();
    final Callback<Object, Object> cb = new Callback<Object, Object>() {
      public Object call(final Object arg) {
        final float t = (float)((float)(System.nanoTime() - start_put) / (float)1000000);
        //LOG.debug(String.format("TSDB: Recording put time of [%f] ms", t));
        IncomingDataPoints.putlatency.addValue(t);
        return arg;
      }
      public String toString() {
        return "time put request";
      }
    };
    
    return data_storage.putWithRetry(row, FAMILY, Bytes.fromShort(qualifier), value,
        null, false, true).addCallback(cb);
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
    try{
      // force sync of the timestamp uids
      if (uid_manager != null){
        uid_manager.interrupt();
        uid_manager = null;
      }
      LOG.trace("Flushing TS UIDs");
      this.ts_uids.processNewMeta(metrics, tag_names, tag_values, timeseries_meta);
      
      LOG.trace("Flushing TS Meta");
      this.timeseries_meta.flush();
      
      LOG.trace("Flushing metric maps");
      this.metrics.flushMeta();
      
      LOG.trace("Flushing tagk maps");
      this.tag_names.flushMeta();
      
      LOG.trace("Flushing tagv maps");
      this.tag_values.flushMeta();
      
      data_storage.flush();
      return uid_storage.flush();
    }catch (NullPointerException npe){
      npe.printStackTrace();
      return null;
    }catch (Exception e){
      e.printStackTrace();
      return null;
    }
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
        data_storage.shutdown();
        return uid_storage.shutdown();
      }
      public String toString() {
        return "shutdown HBase client";
      }
    }
    // First flush the compaction queue, then shutdown the HBase client.
    return enable_compactions
      ? compactionq.flush().addBoth(new HClientShutdown())
      : data_storage.shutdown();
  }

  /**
   * Fetches the entire cache of Metrics
   * @return A sorted list of metrics in HBase
   */
  public final SortedMap<String, byte[]> getMetrics(){
    return metrics.getMap();
  }
  
  /**
   * Fetches the entire cache of tag names
   * @return A sorted list of tag names
   */
  public final SortedMap<String, byte[]> getTagNames(){
    return this.tag_names.getMap();
  }
  
  /**
   * Fetches the entire cache of tag values
   * @return A sorted list of tag values
   */
  public final SortedMap<String, byte[]> getTagValues(){
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
  
  public TimeSeriesMeta getTimeSeriesMeta(final byte[] id, final boolean cache){
    if (id.length <= (short)3){
      LOG.debug("ID was too short");
      return null;
    }
    try{
      TimeSeriesMeta meta = this.timeseries_meta.getTimeSeriesMeta(id, cache);
      if (meta == null)
        meta = new TimeSeriesMeta(id);
      
      // otherwise we need to get the general metas for metrics and tags
      byte[] metricID = MetaDataCache.getMetricID(id);
      //LOG.trace(String.format("Metric ID %s", Arrays.toString(metricID)));
      if (metricID == null){
        LOG.debug(String.format("Unable to get metric meta data for ID [%s]", 
            UniqueId.IDtoString(id)));
        return null;
      }else{
        meta.setMetric(this.metrics.getGeneralMeta(metricID, cache));
        if (meta.getMetric() == null)
          return null;
      }
      
      // tags
      ArrayList<byte[]> tags = MetaDataCache.getTagIDs(id);
      if (tags == null || tags.size() < 1)
        LOG.debug(String.format("Unable to get tag and value metadata for ID [%s]",
            UniqueId.IDtoString(id)));
      else{
        ArrayList<GeneralMeta> tm = new ArrayList<GeneralMeta>();
        int index=0;
        for (byte[] tag : tags){
          if ((index % 2) == 0)
            tm.add(this.tag_names.getGeneralMeta(tag, cache));
          else
            tm.add(this.tag_values.getGeneralMeta(tag, cache));
          index++;
        }
        meta.setTags(tm);
      }
      
      return meta;
    }catch (NoSuchUniqueId nsui){
      
    }
    return null;
  }
  
  public Boolean putMeta(final TimeSeriesMeta meta){
    return this.timeseries_meta.putMeta(meta, false) != null;
  }
  
  public static Boolean isInteger(Object dp){
    if (dp.getClass().equals(Integer.class) || 
        dp.getClass().equals(Long.class) ||
        dp.getClass().equals(Short.class))
      return true;
    else
      return false;
  }
  
  public static Boolean isFloat(Object dp){
    if (dp.getClass().equals(Float.class) ||  
        dp.getClass().equals(double.class))
      return true;
    else
      return false;
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
    /**
     * Runs the thread that handles the UID tasks
     */
    public void run(){
      while(true){
        try {
          Thread.sleep(5000);
          
          if (role == TSDRole.Ingest){
            LOG.debug("Flushing all TS/UID maps and meta...");
            // update the UIDs
            metrics.flushMeta();
            tag_names.flushMeta();
            tag_values.flushMeta();
            timeseries_meta.flush();

            //ts_uids.flush();
            ts_uids.processNewMeta(metrics, tag_names, tag_values, timeseries_meta);
            LOG.debug("Flushed all TS/UID maps and meta");
          }
          
        } catch (InterruptedException e) {
          break;
        } catch (Exception ex){
          ex.printStackTrace();
        }
        
      }
      LOG.error("UID Manager thread has crashed");
    }
  }
  
  private final class TSUIDManager extends Thread {
    public void run(){
      while(true){
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
        LOG.info("Processing TSUID maps and meta");
        ts_uids.processNewMeta(metrics, tag_names, tag_values, timeseries_meta);
        LOG.info("Processed TSUID maps and meta");
      }
    }
  }
}

package net.opentsdb.meta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import net.opentsdb.cache.Cache;
import net.opentsdb.cache.Cache.CacheRegion;
import net.opentsdb.core.JSON;
import net.opentsdb.meta.GeneralMeta.Meta_Type;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.TsdbStoreHBase;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This will contain all of the code to cache, get and set meta data for the
 * different UIDs. It's similar to the UniqueId class.
 * 
 * To keep lookup times as short as possible, we're implementing this alongside
 * the UniqueId class instead of integrating it since it would balloon the
 * memory of the UID maps. Meta data will be used much less frequently than UIDs
 */
public class MetaDataCache {
  private static final Logger LOG = LoggerFactory.getLogger(MetaDataCache.class);

  final private TsdbStore storage;

  /**
   * Whether or not this cache handles Time Series meta data or general True =
   * time series False = general
   */
  private final Boolean is_ts;

  /** The kind of UniqueId, used as the column qualifier. */
  private final String kind;

  /** Queue for storing new entries that should be flushed to storage */
  private final ArrayList<MetaData> storage_queue = new ArrayList<MetaData>();

  private final Cache cache;
  
  /**
   * Constructor.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param kind The name of the cache, e.g. metrics, tagk or ts
   */
  public MetaDataCache(final TsdbStore client, final byte[] table,
      final Boolean is_ts, final String kind, final Cache cache) {
    this.storage = client;
    this.is_ts = is_ts;
    this.kind = kind;
    this.cache = cache;
  }

  public TimeSeriesMeta getTimeSeriesMeta(final byte[] id) {
    TimeSeriesMeta meta = 
      (TimeSeriesMeta) this.cache.get(CacheRegion.META, kind + UniqueId.IDtoString(id));
    if (meta != null)
      return meta;
    meta = getTimeSeriesMetaFromHBase(id);
    if (meta != null) {
      return meta;
    } else
      return null;
  }

  public boolean haveMeta(final String uid){
    if (this.cache != null && 
        this.cache.get(CacheRegion.META, kind + uid) != null)
      return true;
    
    try{
      final String cell = this.kind + "_meta";
      final byte[] raw_meta = storage.getValue(UniqueId.StringtoID(uid), TsdbStore.toBytes("name"),
          TsdbStore.toBytes(cell));
      if (raw_meta != null)
        return true;
    }catch (TsdbStorageException tse){
      tse.printStackTrace();
    }
    return false;
  }
  
  public GeneralMeta getGeneralMeta(final byte[] id) {
    GeneralMeta meta = (GeneralMeta) this.cache.get(CacheRegion.META, kind + UniqueId.IDtoString(id));
    if (meta != null){
      return meta;
    }
    meta = getGeneralMetaFromHBase(id);
    if (meta != null){
      return meta;
    }
    else
      return null;
  }

  /**
   * Merges the metadata with whatever is in storage
   * @param meta
   * @return
   */
  public GeneralMeta putMeta(final MetaData meta) {
    if (meta == null) {
      LOG.error("Null value for meta object");
      return null;
    }

    final String uid = meta.getUID();
    // check for uid
    if (!this.is_ts){
      if (((GeneralMeta) meta).getName().isEmpty()){
        LOG.error("Missing name");
        return null;
      }
    }
    if (uid.length() < 1) {
      LOG.error("Missing UID");
      return null;
    }

    MetaData new_meta = this.flushMeta(meta);
    if (!this.is_ts){
      return (GeneralMeta) new_meta;
    }else
      return null;
  }

  public void QueueMeta(final MetaData meta){
    this.storage_queue.add(meta);
  }
  
  public void putCache(final byte[] id, final MetaData meta){
    this.cache.put(CacheRegion.META, kind + UniqueId.IDtoString(id), meta);
  }
  
  /**
   * Runs through the list, flushes anything to disk that exists, and then wipes the cache
   */
  public final void flush(){
    if (this.storage_queue.isEmpty())
      return;
    
    ArrayList<MetaData> meta_copy = new ArrayList<MetaData>();
    meta_copy.addAll(this.storage_queue);
    this.storage_queue.clear();

    for (MetaData entry : meta_copy){
      this.flushMeta(entry);
    }
    LOG.debug(String.format("Flushed [%d] metadata entries", meta_copy.size()));
    meta_copy.clear();
  }
  
  // STATICS ---------------------------------------------------------

  public static byte[] getMetricID(final byte[] id) {
    if (id.length < 3) {
      LOG.warn("Timeseries ID is too small");
      return null;
    }

    byte[] mid = new byte[3];
    for (int i = 0; i < 3; i++)
      mid[i] = id[i];
    return mid;
  }

  public static ArrayList<byte[]> getTagIDs(final byte[] id) {
    if (id.length < 3) {
      LOG.warn("Timeseries ID is too small");
      return null;
    }
    if (id.length < 9) {
      LOG.warn("No tags found in ID");
      return null;
    }

    ArrayList<byte[]> tags = new ArrayList<byte[]>();
    byte[] tag = new byte[3];
    int x = 0;
    for (int i = 3; i < id.length; i++) {
      if (i > 3 && (i % 3) == 0) {
        tags.add(tag);
        x = 0;
        tag = new byte[3];
      }
      tag[x] = id[i];
      x++;
    }
    tags.add(tag);
    return tags;
  }

  // PRIVATES ---------------------------------------------------------

  private TimeSeriesMeta getTimeSeriesMetaFromHBase(final byte[] id)
      throws TsdbStorageException {
    try{
      final String cell = this.kind + "_meta";
      final byte[] raw_meta = storage.getValue(id, TsdbStore.toBytes("name"),
          TsdbStore.toBytes(cell));
      final String json = (raw_meta == null ? null : TsdbStore
          .fromBytes(raw_meta));
      if (json == null)
        // todo - log
        return null;
  
      JSON codec = new JSON(new TimeSeriesMeta(id));
      if (codec.parseObject(json)) {
        TimeSeriesMeta meta = (TimeSeriesMeta) codec.getObject();
//        if (cache)
//          this.cache.put(CacheRegion.META, kind + UniqueId.IDtoString(id), meta);
        return meta;
      }
    }catch (TsdbStorageException tse){
      tse.printStackTrace();
    }
    // todo - log
    return null;
  }

  private GeneralMeta getGeneralMetaFromHBase(final byte[] id)
      throws TsdbStorageException {
    final String cell = this.kind + "_meta";
    final Meta_Type type;
    if (this.kind.compareTo("metrics") == 0)
      type = Meta_Type.METRICS;
    else if (this.kind.compareTo("tagk") == 0)
      type = Meta_Type.TAGK;
    else
      type = Meta_Type.TAGV;

    final byte[] raw_meta = storage.getValue(id, TsdbStore.toBytes("name"),
        TsdbStore.toBytes(cell));
    final String json = (raw_meta == null ? null : TsdbStore
        .fromBytes(raw_meta));
    if (json != null){
      JSON codec = new JSON(new GeneralMeta(id));
      if (codec.parseObject(json)) {
        final GeneralMeta meta = (GeneralMeta) codec.getObject();
        if (this.kind.compareTo("metrics") == 0)
          meta.setType(Meta_Type.METRICS);
        else if (this.kind.compareTo("tagk") == 0)
          meta.setType(Meta_Type.TAGK);
        else
          meta.setType(Meta_Type.TAGV);
//        if (cache)
//          this.cache.put(UniqueId.IDtoString(id), meta);
        return meta;
      }
    }
    // todo - log
    //this.cache.put(UniqueId.IDtoString(id), new GeneralMeta(id, type));
    return new GeneralMeta(id, type);
  }

  private final MetaData flushMeta(final MetaData meta){
    final String uid = meta.getUID();
    // check for uid
    if (!this.is_ts){
      if (((GeneralMeta) meta).getName().isEmpty()){
        LOG.error("Missing name");
        return null;
      }
    }
    if (uid.length() < 1) {
      LOG.error("Missing UID");
      return null;
    }

    final byte[] id = UniqueId.StringtoID(uid);

    short attempt = 5;
    while (attempt-- > 0) {
      // lock and get the latest from Hbase
      Object lock;
      try {
        lock = storage.getRowLock(id);
      } catch (HBaseException e) {
        try {
          Thread.sleep(61000 / 5);
        } catch (InterruptedException ie) {
          break; // We've been asked to stop here, let's bail out.
        }
        continue;
      } catch (Exception e) {
        throw new RuntimeException("Should never be here", e);
      }
      if (lock == null) { // Should not happen.
        LOG.error("WTF, got a null pointer as a RowLock!");
        continue;
      }

      try {
        MetaData new_meta = meta;
        // fetch from hbase so we know we have the latest value
        final String cell = this.kind + "_meta";
        final byte[] raw = storage.getValue(id, TsdbStore.toBytes("name"),
            TsdbStore.toBytes(cell), lock);

        String json = "";
        if (raw == null) {
          // if nothing existed before, we'll store the user provided data
          if (this.is_ts) {
            json = ((TimeSeriesMeta) meta).getJSON();
          } else {
            LOG.trace("New GMO metadata...");
            ((GeneralMeta) meta).setCreated(System.currentTimeMillis() / 1000L);
            ((GeneralMeta) meta).setType(Meta_Type.INVALID);
            json = ((GeneralMeta) meta).getJSON();
          }
        } else {
          // otherwise, we will copy changes to the new meta entry
          if (this.is_ts) {
            TimeSeriesMeta m = new TimeSeriesMeta();
            JSON codec = new JSON(m);
            if (!codec.parseObject(TsdbStore.fromBytes(raw))) {
              LOG.warn("Error parsing JSON from Hbase for ID [" + uid
                  + "], replacing");
              json = ((TimeSeriesMeta) meta).getJSON();
            } else {
              m = (TimeSeriesMeta) codec.getObject();
              // if no changes, skip
              if (m.equals((TimeSeriesMeta)meta)){
                LOG.debug(String.format("No changes for meta [%s]", uid));
                return m;
              }
              m = (TimeSeriesMeta) ((TimeSeriesMeta) meta).copyChanges((MetaData)m);
              json = m.getJSON();
            }
            new_meta = m;
          } else {
            GeneralMeta m = new GeneralMeta();
            JSON codec = new JSON(m);
            if (!codec.parseObject(TsdbStore.fromBytes(raw))) {
              LOG.warn("Error parsing JSON from Hbase for ID [" + uid
                  + "], replacing");
              // don't want to store the type field
              ((GeneralMeta) meta).setType(Meta_Type.INVALID);
              json = ((GeneralMeta) meta).getJSON();
            } else {
              m = (GeneralMeta) codec.getObject();
              // if no changes, skip
              if (m.equals((GeneralMeta)meta)){
                LOG.debug(String.format("No changes for meta [%s]", uid));
                return m;
              }
              m = (GeneralMeta) ((GeneralMeta) meta).copyChanges(m);
              // don't want to store the type field
              ((GeneralMeta) m).setType(Meta_Type.INVALID);
              json = m.getJSON();
              LOG.trace("GMO [" + ((GeneralMeta) meta).getName() + "] new [" + 
                  m.getName() + "]");
            }
            new_meta = m;
          }
        }

        // put me
        try {
          storage.putWithRetry(id, TsdbStore.toBytes("name"),
              TsdbStore.toBytes(this.kind + "_meta"), TsdbStore.toBytes(json),
              0, lock);
          LOG.debug("Updated meta in storage for [" + this.kind + "] on UID [" + UniqueId.IDtoString(id) + "]");
        } catch (HBaseException e) {
          LOG.error("Failed to Put Meta Data [" + uid + "]: " + e);
          continue;
        }

        return new_meta;
      } finally {
        storage.releaseRowLock(lock);
      }
    }

    LOG.error("Failed to put the meta data");
    return null;
  }
}

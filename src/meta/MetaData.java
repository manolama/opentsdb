package net.opentsdb.meta;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import net.opentsdb.core.JSON;
import net.opentsdb.meta.GeneralMeta.Meta_Type;
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
public class MetaData {
  private static final Logger LOG = LoggerFactory.getLogger(MetaData.class);

  final private TsdbStore storage;

  /**
   * Whether or not this cache handles Time Series meta data or general True =
   * time series False = general
   */
  private final Boolean is_ts;

  /** The kind of UniqueId, used as the column qualifier. */
  private final String kind;

  /** Cache */
  private final ConcurrentHashMap<byte[], Object> cache = new ConcurrentHashMap<byte[], Object>();

  /**
   * Constructor.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param kind The name of the cache, e.g. metrics, tagk or ts
   */
  public MetaData(final HBaseClient client, final byte[] table,
      final Boolean is_ts, final String kind) {
    this.storage = new TsdbStoreHBase(table, client);
    this.is_ts = is_ts;
    this.kind = kind;
  }

  public TimeSeriesMeta getTimeSeriesMeta(final byte[] id) {
    TimeSeriesMeta meta = (TimeSeriesMeta) cache.get(TsdbStore.fromBytes(id));
    if (meta != null)
      return meta;
    meta = getTimeSeriesMetaFromHBase(id);
    if (meta != null) {
      return meta;
    } else
      return new TimeSeriesMeta(id);
  }

  public GeneralMeta getGeneralMeta(final byte[] id) {
    GeneralMeta meta = (GeneralMeta) cache.get(TsdbStore.fromBytes(id));
    if (meta != null)
      return meta;
    meta = getGeneralMetaFromHBase(id);
    if (meta != null)
      return meta;
    else
      return new GeneralMeta(id);
  }

  public Boolean putMeta(final Object meta) {
    if (meta == null) {
      LOG.error("Null value for meta object");
      return false;
    }

    final String uid;
    // check for uid
    if (this.is_ts)
      uid = ((TimeSeriesMeta) meta).getUID();
    else
      uid = ((GeneralMeta) meta).getUID();
    if (uid.length() < 1) {
      LOG.error("Missing UID");
      return false;
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
            ((GeneralMeta) meta).setCreated(System.currentTimeMillis() / 1000L);
            ((GeneralMeta) meta).setName("");
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
              m = ((TimeSeriesMeta) meta).CopyChanges(m);
              json = m.getJSON();
            }
          } else {
            GeneralMeta m = new GeneralMeta();
            JSON codec = new JSON(m);
            if (!codec.parseObject(TsdbStore.fromBytes(raw))) {
              LOG.warn("Error parsing JSON from Hbase for ID [" + uid
                  + "], replacing");
              ((GeneralMeta) meta).setName("");
              ((GeneralMeta) meta).setType(Meta_Type.INVALID);
              json = ((GeneralMeta) meta).getJSON();
            } else {
              m = (GeneralMeta) codec.getObject();
              m = ((GeneralMeta) meta).CopyChanges(m);
              ((GeneralMeta) m).setName("");
              ((GeneralMeta) m).setType(Meta_Type.INVALID);
              json = m.getJSON();
            }
          }
        }

        // put me
        try {
          storage.putWithRetry(id, TsdbStore.toBytes("name"),
              TsdbStore.toBytes(this.kind + "_meta"), TsdbStore.toBytes(json),
              lock);
        } catch (HBaseException e) {
          LOG.error("Failed to Put Meta Data [" + uid + "]", e);
          continue;
        }

        // cacheme
        this.cache.put(id, meta);
        return true;
      } finally {
        storage.releaseRowLock(lock);
      }
    }

    LOG.error("Failed to put the meta data");
    return false;
  }

  // STATICS ---------------------------------------------------------
  
  public static byte[] getMetricID(final byte[] id){
    if (id.length < 3){
      LOG.warn("Timeseries ID is too small");
      return null;
    }
    
    byte[] mid = new byte[3];
    for (int i=0; i<3; i++)
      mid[i] = id[i];
    return mid;
  }
  
  public static ArrayList<byte[]> getTagIDs(final byte[] id){
    if (id.length < 3){
      LOG.warn("Timeseries ID is too small");
      return null;
    }
    if (id.length < 9){
      LOG.warn("No tags found in ID");
      return null;
    }
    
    ArrayList<byte[]> tags = new ArrayList<byte[]>();
    byte[] tag = new byte[3];
    int x =0;
    for (int i=3; i<id.length; i++){
      if (i > 3 && (i % 3) == 0){
        tags.add(tag);
        x=0;
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
      throws HBaseException {
    final String cell = this.kind + "_meta";
    final byte[] raw_meta = storage.getValue(id, TsdbStore.toBytes("name"),
        TsdbStore.toBytes(cell));
    final String json = (raw_meta == null ? null : TsdbStore.fromBytes(raw_meta));
    if (json == null)
      // todo - log
      return null;

    JSON codec = new JSON(new TimeSeriesMeta(id));
    if (codec.parseObject(json)) {
      return (TimeSeriesMeta) codec.getObject();
    }
    // todo - log
    return null;
  }

  private GeneralMeta getGeneralMetaFromHBase(final byte[] id)
      throws HBaseException {
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
    final String json = (raw_meta == null ? null : TsdbStore.fromBytes(raw_meta));
    if (json == null)
      return new GeneralMeta(id, type);

    JSON codec = new JSON(new GeneralMeta(id));
    if (codec.parseObject(json)) {
      final GeneralMeta meta = (GeneralMeta) codec.getObject();
      if (this.kind.compareTo("metrics") == 0)
        meta.setType(Meta_Type.METRICS);
      else if (this.kind.compareTo("tagk") == 0)
        meta.setType(Meta_Type.TAGK);
      else
        meta.setType(Meta_Type.TAGV);
      return meta;
    }
    // todo - log
    return new GeneralMeta(id, type);
  }

}

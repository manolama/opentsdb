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
package net.opentsdb.uid;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaData;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;

/**
 * Thread-safe implementation of the {@link UniqueIdInterface}.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on this class.
 * @see UniqueIdInterface
 */
public final class UniqueId implements UniqueIdInterface {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  private enum ScanType {
    NAME,
    MAP,
    META
  }
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** The single column family used by this class. */
  private static final byte[] ID_FAMILY = toBytes("id");
  /** The single column family used by this class. */
  private static final byte[] NAME_FAMILY = toBytes("name");
  /** Row key of the special row used to track the max ID already assigned. */
  private static final byte[] MAXID_ROW = { 0 };
  /** How many time do we try to assign an ID before giving up. */
  private static final short MAX_ATTEMPTS_ASSIGN_ID = 3;
  /** Maximum number of results to return in suggest(). */
  private static final short MAX_SUGGESTIONS = 25;
  /** How often to reload the map in seconds so we don't keep hitting HBase */
  private static final int RELOAD_INTERVAL = 60;

  /** HBase client to use. */
  private final TsdbStore storage;

  /** Table where IDs are stored. */
  private final byte[] table;
  /** The kind of UniqueId, used as the column qualifier. */
  private final byte[] kind;
  /** Number of bytes on which each ID is encoded. */
  private final short idWidth;

  /** Cache for forward mappings (name to ID). */
  private final ConcurrentHashMap<String, byte[]> nameCache = new ConcurrentHashMap<String, byte[]>();
  /**
   * Cache for backward mappings (ID to name). The ID in the key is a byte[]
   * converted to a String to be Comparable.
   */
  private final ConcurrentHashMap<String, String> idCache = new ConcurrentHashMap<String, String>();

  /** Number of times we avoided reading from HBase thanks to the cache. */
  private volatile int cacheHits;
  /** Number of times we had to read from HBase and populate the cache. */
  private volatile int cacheMisses;

  private volatile Set<byte[]> bad_meta_ids = new HashSet<byte[]>();
  
  /** Last time this map was loaded in it's entirety */
  private long last_full_uid_load = 0;
  private long last_full_map_load = 0;
  private long last_full_meta_load = 0;

  /** Metadata associated with this UID */
  private final MetaData metadata;

  /** UID map associated with this object */
  private final ConcurrentHashMap<String, UniqueIdMap> uid_map = new ConcurrentHashMap<String, UniqueIdMap>();

  /**
   * Constructor.
   * @param client The HBase client to use.
   * @param table The name of the HBase table to use.
   * @param kind The kind of Unique ID this instance will deal with.
   * @param width The number of bytes on which Unique IDs should be encoded.
   * @throws IllegalArgumentException if width is negative or too small/large or
   *           if kind is an empty string.
   */
  public UniqueId(final TsdbStore client, final byte[] table,
      final String kind, final int width) {
    this.table = table;
    this.storage = client;
    if (kind.isEmpty()) {
      throw new IllegalArgumentException("Empty string as 'kind' argument!");
    }
    this.kind = toBytes(kind);
    if (width < 1 || width > 8) {
      throw new IllegalArgumentException("Invalid width: " + width);
    }
    this.idWidth = (short) width;
    this.metadata = new MetaData(client, table, false, kind);
  }

  // we need to pad so that we have a 3 byte ID
  public static String IDtoString(final byte[] id) {
    String sid = DatatypeConverter.printHexBinary(id);
    return sid;
  }

  public static byte[] StringtoID(final String id) {
    if (id == null)
      return null;
    
    // check padding
    String sid = id;
    if (sid.length() % 2 > 0)
      sid = "0" + sid;
    while (sid.length() < 6) {
      sid = "0" + sid;
    }
    return DatatypeConverter.parseHexBinary(sid);
  }

  /**
   * Extracts the metric UID from the row key
   * @param key The row key to parse
   * @param metric_width The width of the metric tag in bytes
   * @return The metric UID
   */
  public static byte[] getMetricFromKey(final byte[] key,
      final short metric_width) {
    byte[] metric = new byte[metric_width];
    for (int i = 0; i < metric_width; i++) {
      metric[i] = key[i];
    }
    return metric;
  }

  /**
   * Extracts the timestamp from a row key
   * @param key The row key to parse
   * @param metric_width The width of the metric tag in bytes
   * @param timestamp_width The width of the timestamp in bytes
   * @return The timestamp as a byte array to be converted by the caller
   */
  public static byte[] getTimestampFromKey(final byte[] key,
      final short metric_width, final short timestamp_width) {
    int x = 0;
    byte[] timestamp = new byte[timestamp_width];
    for (int i = metric_width; i < timestamp_width + metric_width; i++) {
      timestamp[x] = key[i];
      x++;
    }
    return timestamp;
  }

  /**
   * Extracts a list of tagk/tagv pairs from a row key
   * @param key The row key to parse
   * @param metric_width The width of the metric tag in bytes
   * @param tag_width The width of the tags in bytes
   * @param timestamp_width The width of the timestamp in bytes
   * @return A list of tagk/tagv pairs as a single byte array
   */
  public static List<byte[]> getTagPairsFromKey(final byte[] key,
      final short metric_width, final short tag_width, final short timestamp_width) {
    int x = 0;
    List<byte[]> tags = new ArrayList<byte[]>();
    byte[] tag = new byte[tag_width * 2];
    for (int i = metric_width + timestamp_width; i < key.length; i++) {
      tag[x] = key[i];
      x++;
      if (x == tag_width * 2) {
        tags.add(tag);
        tag = new byte[tag_width * 2];
        x = 0;
      }
    }
    return tags;
  }

  /**
   * Extracts a list of tagk names from the tagk/tagv pair list of byte arrays
   * See getTagPairsFromKey
   * @param tags List of tagk/tagv pairs to parse
   * @param tag_width Width of a single tag in bytes
   * @return A list of tagk UIDs
   */
  public static List<byte[]> getTagksFromTagPairs(final List<byte[]> tags,
      final short tag_width) {
    List<byte[]> tagks = new ArrayList<byte[]>();
    for (byte[] pair : tags) {
      byte[] tagk = new byte[tag_width];
      for (int i = 0; i < tag_width; i++) {
        tagk[i] = pair[i];
      }
      tagks.add(tagk);
    }

    return tagks;
  }

  /**
   * Extracts a list of tagv names from the tagk/tagv pair list of byte arrays
   * See getTagPairsFromKey
   * @param tags List of tagk/tagv pairs to parse
   * @param tag_width Width of a single tag in bytes
   * @return A list of tagv UIDs
   */
  public static List<byte[]> getTagvsFromTagPairs(final List<byte[]> tags,
      final short tag_width) {
    
    List<byte[]> tagvs = new ArrayList<byte[]>();
    for (byte[] pair : tags) {
      if (pair.length < tag_width * 2 || pair.length < 1){
        LOG.error(String.format("Improper tag pair, length expected [%d], got [%d]",
            tag_width, pair.length));
        return null;
      }

      int x = 0;
      byte[] tagv = new byte[tag_width];
      for (int i = tag_width; i < pair.length; i++) {
        tagv[x] = pair[i];
        x++;
      }
      tagvs.add(tagv);
    }
    return tagvs;
  }

  /**
   * Extracts the timestamp UID from the key, basically just strips the timestamp
   * @param key The row key to parse
   * @param metric_width The width of the metric tag in bytes
   * @param timestamp_width The width of the timestamp in bytes
   * @return The Timestamp UID
   */
  public static byte[] getTSUIDFromKey(final byte[] key, final short metric_width, 
      final short timestamp_width){
    int x=0;
    byte[] uid = new byte[key.length - timestamp_width];
    for (int i = 0; i < key.length; i++) {
      if (i < metric_width || i >= metric_width + timestamp_width){
        uid[x] = key[i];
        x++;
      }
    }
    return uid;
  }
  
  /** The number of times we avoided reading from HBase thanks to the cache. */
  public int cacheHits() {
    return cacheHits;
  }

  /** The number of times we had to read from HBase and populate the cache. */
  public int cacheMisses() {
    return cacheMisses;
  }

  /** Returns the number of elements stored in the internal cache. */
  public int cacheSize() {
    return nameCache.size() + idCache.size();
  }

  public String kind() {
    return fromBytes(kind);
  }

  public short width() {
    return idWidth;
  }

  public String getName(final byte[] id) throws NoSuchUniqueId, HBaseException {
    if (id.length != idWidth) {
      throw new IllegalArgumentException("Wrong id.length = " + id.length
          + " which is != " + idWidth + " required for '" + kind() + '\'');
    }
    String name = getNameFromCache(id);
    if (name != null) {
      cacheHits++;
    } else {
      cacheMisses++;
      name = getNameFromHBase(id);
      if (name == null) {
        throw new NoSuchUniqueId(kind(), id);
      }
      addNameToCache(id, name);
      addIdToCache(name, id);
    }
    return name;
  }

  private String getNameFromCache(final byte[] id) {
    return idCache.get(fromBytes(id));
  }

  private String getNameFromHBase(final byte[] id) throws HBaseException {
//    LOG.trace(String.format("ID as string [%s] from byte [%s] kind [%s] fam [%s]", 
//        IDtoString(id), Arrays.toString(id), fromBytes(kind), fromBytes(NAME_FAMILY)));
    final byte[] name = storage.getValue(id, NAME_FAMILY, kind);
    return name == null ? null : fromBytes(name);
  }

  private void addNameToCache(final byte[] id, final String name) {
    final String key = fromBytes(id);
    String found = idCache.get(key);
    if (found == null) {
      found = idCache.putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + Arrays.toString(id) + " => name="
          + name + ", already mapped to " + found);
    }
  }

  public byte[] getId(final String name) throws NoSuchUniqueName,
      TsdbStorageException {
    byte[] id = getIdFromCache(name);
    if (id != null) {
      cacheHits++;
    } else {
      cacheMisses++;
      id = getIdFromStorage(name);
      if (id == null) {
        throw new NoSuchUniqueName(kind(), name);
      }
      if (id.length != idWidth) {
        LOG.trace(String.format("Invalid id [%s]", IDtoString(id)));
        throw new IllegalStateException("Found id.length = " + id.length
            + " which is != " + idWidth + " required for '" + kind() + '\'');
      }
      addIdToCache(name, id);
      addNameToCache(id, name);
    }
    return id;
  }

  private byte[] getIdFromCache(final String name) {
    return nameCache.get(name);
  }

  private byte[] getIdFromStorage(final String name)
      throws TsdbStorageException {
//    LOG.trace(String.format("Fetching ID for name [%s] and kind [%s] table [%s] fam [%s]", 
//        name, fromBytes(kind), storage.getTableString(), fromBytes(ID_FAMILY)));
    return storage.getValue(toBytes(name), ID_FAMILY, kind);
  }

  private void addIdToCache(final String name, final byte[] id) {
    byte[] found = nameCache.get(name);
    if (found == null) {
      found = nameCache.putIfAbsent(name,
      // Must make a defensive copy to be immune
      // to any changes the caller may do on the
      // array later on.
          Arrays.copyOf(id, id.length));
    }
    if (found != null && !Arrays.equals(found, id)) {
      throw new IllegalStateException("name=" + name + " => id="
          + Arrays.toString(id) + ", already mapped to "
          + Arrays.toString(found));
    }
  }

  public GeneralMeta getGeneralMeta(final byte[] id) {
    try{
      if (this.bad_meta_ids.contains(id)){
        LOG.trace("ID [" + IDtoString(id) + "] was in the bad list");
        return null;
      }
      GeneralMeta meta = this.metadata.getGeneralMeta(id);
      if (meta.getName().length() < 1 && meta.getCreated() < 1){
        LOG.trace(String.format("Didn't find %s metatdata for UID [%s]",
            fromBytes(kind), IDtoString(id)));
        String name = this.getName(id);
        if (name == null || name.isEmpty()){
          LOG.error(String.format("%s UID [%s] does not exist in storage", fromBytes(kind), 
              IDtoString(id)));
          this.bad_meta_ids.add(id);
          return null;
        }
        //LOG.trace("Saving " + name);
        meta.setName(name);
        // todo - do we want to put here?
        //this.metadata.putMeta(meta);
      }
      // temp
      if (meta.getName().isEmpty())
        meta.setName(this.getName(id));
      return meta;
    }catch (NoSuchUniqueId nuid){
      LOG.error(String.format("%s UID [%s] does not exist in storage", fromBytes(kind), 
          IDtoString(id)));
      this.bad_meta_ids.add(id);
      return null;
    }
  }

  public Boolean putMeta(final GeneralMeta meta) {
    return this.metadata.putMeta(meta);
  }

  public Boolean putMap(final String uid, final String value, final String type){
    if (uid == null || uid.length() < 1){
      LOG.error("Missing or null UID");
      return false;
    }
    
    // first, see if the UID exists
    try{
      String name = this.getName(StringtoID(uid));
    } catch (NoSuchUniqueId nid){
      LOG.error(String.format("Unable to find UID [%s] for type [%s]", 
          uid, fromBytes(kind)));
      return false;
    }
    
    // see if a map exists, if not, create a new one
    UniqueIdMap map = getMap(uid);
    if (map == null){
      map = new UniqueIdMap(uid);
    }
    map.putMap(value, type);
    uid_map.put(uid, map);
    return true;
  }
  
  /**
   * Runs through the maps and flushes any to the storage system that have updates
   */
  public void flushMaps(final Boolean use_local){
    long changed = 0;
    try{
      for (Map.Entry<String, UniqueIdMap> entry : this.uid_map.entrySet()){
        UniqueIdMap map = entry.getValue();
        if (!map.getHasChanged())
          continue;
        
        String uid_str = map.getUid();
        if (uid_str == null){
          LOG.error(String.format("Null UID for %s map ID [%s]", fromBytes(kind), entry.getKey()));
          continue;
        }
        
        // lock, fetch, merge, put
        byte[] uid = StringtoID(uid_str);
        byte[] qualifier = toBytes(fromBytes(kind) + "_map");
        short attempt = 3;
        Object lock = null;
        try{
          while(attempt-- > 0){
            LOG.debug(String.format("Attempting to sync map for %s [%s]", 
                fromBytes(kind), map.getUid()));
            // first, we need to lock the row for exclusive access on the set
            try {
              lock = storage.getRowLock(uid);          
              if (lock == null) {
                LOG.error("Received null for row lock");
                continue;
              }
              LOG.debug(String.format("Successfully locked UID row [%s]", map.getUid()));
              
              UniqueIdMap temp_map = new UniqueIdMap(map.getUid());
              JSON codec = new JSON(temp_map);
              
              // get the current value from storage so we don't overwrite other TSDs changes
              byte[] smap = storage.getValue(uid, NAME_FAMILY, qualifier, lock);
              if (smap == null){
                LOG.warn(String.format("UID map for [%s] was not found in the storage system", 
                    fromBytes(kind)));
              }else{
                if (!codec.parseObject(TsdbStore.fromBytes(smap))){
                  LOG.error("Unable to parse UID map from the storage system");
                  return;
                }
                temp_map = (UniqueIdMap)codec.getObject();
                LOG.debug("Successfully loaded UID map from the storage system");
                
                // now we compare the newly loaded list and the old one, if there are any differences,
                // we need to update storage
                if (map.equals(temp_map)){
                  LOG.debug("No changes from stored data");
                  return;
                }
              }          
              
              // there was a difference so merge the two sets, then write to storage
              if (!use_local)
                map.merge(temp_map);
              LOG.trace(String.format("%s UID [%s] requires updating",
                  fromBytes(kind), map.getUid()));            
              
              codec = new JSON(map);
              storage.putWithRetry(uid, NAME_FAMILY, qualifier, codec.getJsonBytes(), lock)
                  .joinUninterruptibly();
              LOG.info("Successfully updated UID map in storage");
              // do NOT forget to unlock
              LOG.trace("Releasing lock");
              if (storage.releaseRowLock(lock))
                lock = null;
              changed++;
            } catch (TsdbStorageException e) {
              try {
                Thread.sleep(61000 / 3);
              } catch (InterruptedException ie) {
                return;
              }
              continue;
            } catch (Exception e){
              LOG.error(String.format("Unhandled exception [%s]", e));
              e.printStackTrace();
              return;
            }
          }
        }catch (TsdbStorageException tex){
          LOG.warn(String.format("Exception from storage [%s]", tex.getMessage()));
          return;
        } finally {
          LOG.trace("Releasing lock");
          if (storage.releaseRowLock(lock))
            lock = null;
        }
      }
    }catch (Exception e){
      e.printStackTrace();
      return;
    }
    LOG.trace(String.format("Updated [%d] out of [%d] %s maps", changed, 
        uid_map.size(), fromBytes(kind)));
  }

  /**
   * Attempts to return the map for a UID from cache, then storage
   * @param id The ID of the object to retrieve as a hex encoded uid
   * @return A UniqueIdMap if successful, null if it wasn't found
   */
  public UniqueIdMap getMap(final String id) {
    UniqueIdMap map = this.uid_map.get(id);
    if (map != null)
      return map;
    return getMapFromStorage(id);
  }

  public byte[] getOrCreateId(String name) throws HBaseException {
    short attempt = MAX_ATTEMPTS_ASSIGN_ID;
    TsdbStorageException hbe = null;

    while (attempt-- > 0) {
      try {
        return getId(name);
      } catch (NoSuchUniqueName e) {
        LOG.info("Creating an ID for kind='" + kind() + "' name='" + name
            + '\'');
      }

      // The dance to assign an ID.
      Object lock;
      try {
        lock = storage.getRowLock(MAXID_ROW);
      } catch (TsdbStorageException e) {
        try {
          Thread.sleep(61000 / MAX_ATTEMPTS_ASSIGN_ID);
        } catch (InterruptedException ie) {
          break; // We've been asked to stop here, let's bail out.
        }
        hbe = e;
        continue;
      }
      if (lock == null) { // Should not happen.
        LOG.error("WTF, got a null pointer as a RowLock!");
        continue;
      }
      // We now have hbase.regionserver.lease.period ms to complete the loop.

      try {
        // Verify that the row still doesn't exist (to avoid re-creating it if
        // it got created before we acquired the lock due to a race condition).
        try {
          final byte[] id = getId(name);
          LOG.info("Race condition, found ID for kind='" + kind() + "' name='"
              + name + '\'');
          return id;
        } catch (NoSuchUniqueName e) {
          // OK, the row still doesn't exist, let's create it now.
        }

        // Assign an ID.
        long id; // The ID.
        byte row[]; // The same ID, as a byte array.
        try {
          // We want to send an ICV with our explicit RowLock, but HBase's RPC
          // interface doesn't expose this interface. Since an ICV would
          // attempt to lock the row again, and we already locked it, we can't
          // use ICV here, we have to do it manually while we hold the RowLock.
          // To be fixed by HBASE-2292.
          { // HACK HACK HACK
            {
              final byte[] current_maxid = storage.getValue(MAXID_ROW,
                  ID_FAMILY, kind, lock);
              if (current_maxid != null) {
                if (current_maxid.length == 8) {
                  id = Bytes.getLong(current_maxid) + 1;
                } else {
                  throw new IllegalStateException("invalid current_maxid="
                      + Arrays.toString(current_maxid));
                }
              } else {
                id = 1;
              }
              row = Bytes.fromLong(id);
            }
            // final PutRequest update_maxid = new PutRequest(
            // table, MAXID_ROW, ID_FAMILY, kind, row, lock);
            // hbasePutWithRetry(update_maxid, MAX_ATTEMPTS_PUT,
            // INITIAL_EXP_BACKOFF_DELAY);
            storage.putWithRetry(MAXID_ROW, ID_FAMILY, kind, row, lock);
          } // end HACK HACK HACK.
          LOG.info("Got ID=" + id + " for kind='" + kind() + "' name='" + name
              + "'");
          // row.length should actually be 8.
          if (row.length < idWidth) {
            throw new IllegalStateException("OMG, row.length = " + row.length
                + " which is less than " + idWidth + " for id=" + id + " row="
                + Arrays.toString(row));
          }
          // Verify that we're going to drop bytes that are 0.
          for (int i = 0; i < row.length - idWidth; i++) {
            if (row[i] != 0) {
              final String message = "All Unique IDs for " + kind() + " on "
                  + idWidth + " bytes are already assigned!";
              LOG.error("OMG " + message);
              throw new IllegalStateException(message);
            }
          }
          // Shrink the ID on the requested number of bytes.
          row = Arrays.copyOfRange(row, row.length - idWidth, row.length);
        } catch (TsdbStorageException e) {
          LOG.error(
              "Failed to assign an ID, ICV on row="
                  + Arrays.toString(MAXID_ROW) + " column='"
                  + fromBytes(ID_FAMILY) + ':' + kind() + '\'', e);
          hbe = e;
          continue;
        } catch (IllegalStateException e) {
          throw e; // To avoid handling this exception in the next `catch'.
        } catch (Exception e) {
          LOG.error("WTF?  Unexpected exception type when assigning an ID,"
              + " ICV on row=" + Arrays.toString(MAXID_ROW) + " column='"
              + fromBytes(ID_FAMILY) + ':' + kind() + '\'', e);
          continue;
        }
        // If we die before the next PutRequest succeeds, we just waste an ID.

        // Create the reverse mapping first, so that if we die before creating
        // the forward mapping we don't run the risk of "publishing" a
        // partially assigned ID. The reverse mapping on its own is harmless
        // but the forward mapping without reverse mapping is bad.
        try {
          // final PutRequest reverse_mapping = new PutRequest(
          // table, row, NAME_FAMILY, kind, toBytes(name));
          // hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
          // INITIAL_EXP_BACKOFF_DELAY);
          storage.putWithRetry(row, NAME_FAMILY, kind, toBytes(name), null);
        } catch (TsdbStorageException e) {
          LOG.error("Failed to Put reverse mapping!  ID leaked: " + id, e);
          hbe = e;
          continue;
        }

        // Now create the forward mapping.
        try {
          // final PutRequest forward_mapping = new PutRequest(
          // table, toBytes(name), ID_FAMILY, kind, row);
          // hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
          // INITIAL_EXP_BACKOFF_DELAY);
          storage.putWithRetry(toBytes(name), ID_FAMILY, kind, row, null);
        } catch (TsdbStorageException e) {
          LOG.error("Failed to Put forward mapping!  ID leaked: " + id, e);
          hbe = e;
          continue;
        }

        addIdToCache(name, row);
        addNameToCache(row, name);

        // if we are a tag name or metrics ID, add a meta entry
        if (this.kind().compareTo("metrics") == 0
            || this.kind().compareTo("tagk") == 0) {
          GeneralMeta meta = new GeneralMeta();
          meta.setUID(UniqueId.IDtoString(row));
          meta.setCreated(System.currentTimeMillis() / 1000L);
          meta.setName(name);
          this.metadata.putMeta(meta);
        }

        return row;
      } finally {
        storage.releaseRowLock(lock);
      }
    }
    if (hbe == null) {
      throw new IllegalStateException("Should never happen!");
    }
    LOG.error("Failed to assign an ID for kind='" + kind() + "' name='" + name
        + "'", hbe);
    throw hbe;
  }

  /**
   * Attempts to find suggestions of names given a search term.
   * @param search The search term (possibly empty).
   * @return A list of known valid names that have UIDs that sort of match the
   *         search term. If the search term is empty, returns the first few
   *         terms.
   * @throws HBaseException if there was a problem getting suggestions from
   *           HBase.
   */
  public List<String> suggest(final String search) throws HBaseException {
    // TODO(tsuna): Add caching to try to avoid re-scanning the same thing.
    final TsdbScanner scanner = getSuggestScanner(search);
    final LinkedList<String> suggestions = new LinkedList<String>();
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final byte[] key = row.get(0).key();
          final String name = fromBytes(key);
          final byte[] id = row.get(0).value();
          final byte[] cached_id = nameCache.get(name);
          if (cached_id == null) {
            addIdToCache(name, id);
            addNameToCache(id, name);
          } else if (!Arrays.equals(id, cached_id)) {
            throw new IllegalStateException("WTF?  For kind=" + kind()
                + " name=" + name + ", we have id="
                + Arrays.toString(cached_id)
                + " in cache, but just scanned id=" + Arrays.toString(id));
          }
          suggestions.add(name);
          if ((short) suggestions.size() > MAX_SUGGESTIONS) {
            break;
          }
        }
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
    return suggestions;
  }

  /**
   * Loads the cache with all entries from HBase. To prevent too many hits on
   * the region servers, it will only load every RELOAD_INTERVAL
   * @return True if the load was successful, false if there was an error
   */
  public boolean loadAllUIDs() {
    // determine if we need to load yet
    if ((System.currentTimeMillis() / 1000) < this.last_full_uid_load
        + RELOAD_INTERVAL)
      return true;

    this.last_full_uid_load = System.currentTimeMillis() / 1000;

    final TsdbScanner scanner = getFullScanner(ScanType.NAME);
    try {
      long count=0;
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final byte[] key = row.get(0).key();
          final String name = fromBytes(key);
          final byte[] id = row.get(0).value();
          final byte[] cached_id = nameCache.get(name);
          if (cached_id == null) {
            addIdToCache(name, id);
            addNameToCache(id, name);
          } else if (!Arrays.equals(id, cached_id)) {
            throw new IllegalStateException("WTF?  For kind=" + kind()
                + " name=" + name + ", we have id="
                + Arrays.toString(cached_id)
                + " in cache, but just scanned id=" + Arrays.toString(id));
          }
          count++;
        }
      }
      LOG.trace(String.format("Loaded [%d] uids for [%s]", count, fromBytes(kind)));
      return true;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public boolean LoadAllMeta() {
    // determine if we need to load yet
    if ((System.currentTimeMillis() / 1000) < this.last_full_meta_load
        + RELOAD_INTERVAL)
      return true;

    this.last_full_meta_load = System.currentTimeMillis() / 1000;

    GeneralMeta meta = new GeneralMeta(new byte[] {0});
    JSON codec = new JSON(meta);
    final TsdbScanner scanner = getFullScanner(ScanType.META);
    try {
      long count=0;
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          meta = new GeneralMeta(row.get(0).key());
          if (!codec.parseObject(row.get(0).value())){
            LOG.error(String.format("Unable to parse metadata for [%s]", IDtoString(row.get(0).key())));
            continue;
          }
          meta = (GeneralMeta)codec.getObject();
          this.metadata.putCache(row.get(0).key(), meta);
          count++;
        }
      }
      LOG.trace(String.format("Loaded [%d] metas for [%s]", count, fromBytes(kind)));
      return true;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }

  public boolean loadAllMaps() {
 // determine if we need to load yet
    if ((System.currentTimeMillis() / 1000) < this.last_full_map_load
        + RELOAD_INTERVAL)
      return true;

    this.last_full_map_load = System.currentTimeMillis() / 1000;

    UniqueIdMap map = new UniqueIdMap("");
    JSON codec = new JSON(map);
    final TsdbScanner scanner = getFullScanner(ScanType.MAP);
    try {
      long count=0;
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final byte[] key = row.get(0).key();
          map = new UniqueIdMap("");
          if (!codec.parseObject(row.get(0).value())){
            LOG.error(String.format("Unable to parse map for [%s]", IDtoString(key)));
            continue;
          }
          this.uid_map.put(IDtoString(key), (UniqueIdMap)codec.getObject());
          count++;
        }
      }
      LOG.trace(String.format("Loaded [%d] maps for [%s]", count, fromBytes(kind)));
      return true;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Retrieves the entire list of entries in the cache as a sorted map for
   * display in the HTTP API
   * @return A sorted map of UIDs and their numeric IDs
   */
  public final TreeMap<String, Long> getMap() {
    loadAllUIDs();
    final TreeMap<String, Long> sorted = new TreeMap<String, Long>();
    for (Map.Entry<String, byte[]> entry : this.nameCache.entrySet()) {
      // TODO fix this, it's not returning the actual numeric ID
      if (entry.getValue() == null || entry.getValue().length != 8)
        sorted.put(entry.getKey(), 0L);
      else
        sorted.put(entry.getKey(), Bytes.getLong(entry.getValue()) + 1);
    }
    if (sorted.size() < 1)
      LOG.warn("No metrics found in HBase");
    return sorted;
  }

  /**
   * Reassigns the UID to a different name (non-atomic).
   * <p>
   * Whatever was the UID of {@code oldname} will be given to {@code newname}.
   * {@code oldname} will no longer be assigned a UID.
   * <p>
   * Beware that the assignment change is <b>not atommic</b>. If two threads or
   * processes attempt to rename the same UID differently, the result is
   * unspecified and might even be inconsistent. This API is only here for
   * administrative purposes, not for normal programmatic interactions.
   * @param oldname The old name to rename.
   * @param newname The new name.
   * @throws NoSuchUniqueName if {@code oldname} wasn't assigned.
   * @throws IllegalArgumentException if {@code newname} was already assigned.
   * @throws HBaseException if there was a problem with HBase while trying to
   *           update the mapping.
   */
  public void rename(final String oldname, final String newname) {
    final byte[] row = getId(oldname);
    {
      byte[] id = null;
      try {
        id = getId(newname);
      } catch (NoSuchUniqueName e) {
        // OK, we don't want the new name to be assigned.
      }
      if (id != null) {
        throw new IllegalArgumentException("When trying rename(\"" + oldname
            + "\", \"" + newname + "\") on " + this + ": new name already"
            + " assigned ID=" + Arrays.toString(id));
      }
    }

    final byte[] newnameb = toBytes(newname);

    // Update the reverse mapping first, so that if we die before updating
    // the forward mapping we don't run the risk of "publishing" a
    // partially assigned ID. The reverse mapping on its own is harmless
    // but the forward mapping without reverse mapping is bad.
    try {
      storage.putWithRetry(row, NAME_FAMILY, kind, newnameb, null);
      // final PutRequest reverse_mapping = new PutRequest(
      // table, row, NAME_FAMILY, kind, newnameb);
      // hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
      // INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname + "\", \"" + newname
          + "\") on " + this + ": Failed to update reverse"
          + " mapping for ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Now create the new forward mapping.
    try {
      storage.putWithRetry(newnameb, NAME_FAMILY, kind, row, null);
      // final PutRequest forward_mapping = new PutRequest(
      // table, newnameb, ID_FAMILY, kind, row);
      // hbasePutWithRetry(forward_mapping, MAX_ATTEMPTS_PUT,
      // INITIAL_EXP_BACKOFF_DELAY);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname + "\", \"" + newname
          + "\") on " + this + ": Failed to create the"
          + " new forward mapping with ID=" + Arrays.toString(row), e);
      throw e;
    }

    // Update cache.
    addIdToCache(newname, row); // add new name -> ID
    idCache.put(fromBytes(row), newname); // update ID -> new name
    nameCache.remove(oldname); // remove old name -> ID

    // Delete the old forward mapping.
    try {
      // final DeleteRequest dr = new DeleteRequest(
      // table, toBytes(oldname), ID_FAMILY, kind);
      // client.delete(dr).joinUninterruptibly();
      storage.deleteValue(toBytes(oldname), ID_FAMILY, kind);
    } catch (HBaseException e) {
      LOG.error("When trying rename(\"" + oldname + "\", \"" + newname
          + "\") on " + this + ": Failed to remove the"
          + " old forward mapping for ID=" + Arrays.toString(row), e);
      throw e;
    } catch (Exception e) {
      final String msg = "Unexpected exception when trying rename(\"" + oldname
          + "\", \"" + newname + "\") on " + this + ": Failed to remove the"
          + " old forward mapping for ID=" + Arrays.toString(row);
      LOG.error("WTF?  " + msg, e);
      throw new RuntimeException(msg, e);
    }
    // Success!
  }

  /**
   * Loads all UIDs, Maps (and if applicable, meta) for this type, then scans for matches.
   * If a match is made on the proper field, it will put the metric UID (if this is a 
   * metrics cache) or tag/value pair (if tagk or tagv) in the "matches" set. The set will
   * be used to filter on the tsuids. 
   * 
   * NOTE: If the field is set, then the metadata will be loaded
   * @param field The field to match on. 
   * @param regex The regex to match with
   * @param custom A list of metadata custom tags to filter on
   * @param matches Set of UID or UID pairs that had data matching the regex
   */
  public void search(final String field, final Pattern regex, final Map<String, Pattern> custom, 
      Set<String> matches){  
    // load all metrics so we can scan
    this.loadAllUIDs();
    this.loadAllMaps();
    Boolean load_meta = false;
    if (field.compareTo(fromBytes(kind)) != 0 || custom.size() > 0){
      this.LoadAllMeta();
      load_meta = true;
    }
    GeneralMeta meta = null;
    
    // scan!
    for (String name : this.nameCache.keySet()){
      Boolean match = false;
      String uid = IDtoString(this.nameCache.get(name));
      if (load_meta){
        meta = this.metadata.getGeneralMeta(this.nameCache.get(name));
      }
      
      // if the regex is null AND we don't have a custom filter, just return the data 
      // since we don't have to do any processing
      if (regex == null && custom.size() < 1)
        match = true;      
      else{ 
        // otherwise, we need to check all or one field
        if (regex != null){
          if ((field.compareTo("all") == 0 || field.compareTo(fromBytes(kind)) == 0)
              && regex.matcher(name).find()){
            LOG.trace(String.format("Matched [%s] UID [%s] name [%s]", fromBytes(kind),
               uid, name));
            match = true;
          }

          if ((field.compareTo("all") == 0 || field.compareTo("display_name") == 0)
              && meta != null && regex.matcher(meta.getDisplay_name()).find()){
            LOG.trace(String.format("Matched [%s] UID [%s] display name [%s]", fromBytes(kind),
               uid, meta.getDisplay_name()));
            match = true;
          }
          
          if ((field.compareTo("all") == 0 || field.compareTo("description") == 0)
              && meta != null && regex.matcher(meta.getDescription()).find()){
            LOG.trace(String.format("Matched [%s] UID [%s] description [%s]", fromBytes(kind),
               uid, meta.getDescription()));
            match = true;
          }
          
          if ((field.compareTo("all") == 0 || field.compareTo("notes") == 0)
              && meta != null && regex.matcher(meta.getNotes()).find()){
            LOG.trace(String.format("Matched [%s] UID [%s] notes [%s]", fromBytes(kind),
               uid, meta.getNotes()));
            match = true;
          }
          
          if (field.compareTo("all") == 0 && meta.getCustom() != null){
            Map<String, String> custom_tags = meta.getCustom();
            for (Map.Entry<String, String> tag : custom_tags.entrySet()){
              if (regex.matcher(tag.getKey()).find()){
                LOG.trace(String.format("Matched [%s] custom tag [%s] for uid [%s]",
                    fromBytes(kind), tag.getKey(), uid));
                match = true;
                break;
              }
              if (regex.matcher(tag.getValue()).find()){
                LOG.trace(String.format("Matched [%s] custom tag value [%s] for uid [%s]",
                    fromBytes(kind), tag.getKey(), uid));
                match = true;
                break;
              }
            }
          }
        }

        if (custom != null && custom.size() > 0 && meta != null){
          match = false;
          Map<String, String> custom_tags = meta.getCustom();
          if (custom_tags != null && custom_tags.size() > 0){
            int matched = 0;
            for (Map.Entry<String, Pattern> entry : custom.entrySet()){
              for (Map.Entry<String, String> tag : custom_tags.entrySet()){
                if (tag.getKey().toLowerCase().compareTo(entry.getKey().toLowerCase()) == 0
                    && entry.getValue().matcher(tag.getValue()).find()){
                  LOG.trace(String.format("Matched custom tag [%s] on filter [%s] with value [%s]",
                      tag.getKey(), entry.getValue().toString(), tag.getValue()));
                  matched++;
                }
              }
            }
            if (matched != custom.size()){
              LOG.trace(String.format("%s UID [%s] did not match all of the custom tag filters", 
                  fromBytes(kind), uid));
            }else
              match = true;
          }
        }
      }
      
      // if no match, just move on
      if (!match)
        continue;

      // only return the metric OR tag/value pairs
      if (fromBytes(kind).compareTo("metrics") == 0)
        matches.add(uid);
      else{
        UniqueIdMap map = this.getMap(uid);
        if (map == null)
          continue;
        
        Set<String> pairs = map.getTags();
        if (pairs == null)
          continue;
        for (String pair : pairs){
          if (fromBytes(kind).compareTo("tagk") == 0
              && pair.substring(0, 6).compareTo(uid) == 0)
            matches.add(pair);
          else if (fromBytes(kind).compareTo("tagv") == 0
              && pair.substring(6).compareTo(uid) == 0)
            matches.add(pair);
        }
      }
    }
  }
  
  /**
   * Scans the entire ID cf to see if a name has the matching ID
   * @param id UID to search for
   * @return null if the ID wasn't found, a string with the name of the 
   * metric, tag or value
   */
  public String findMissingID(final byte[] id){
    final TsdbScanner scanner = getFullScanner(ScanType.NAME);
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (Arrays.equals(id, row.get(0).value())){
            return fromBytes(row.get(0).key());
          }
        }
      }
      LOG.trace(String.format("Could not find a matching ID for [%s] in [%s]", 
          IDtoString(id), fromBytes(kind)));
      return null;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Attempts to retrieve the map from storage This method will also add the map
   * to the cache if it's found
   * @param id The ID of the object to retrieve as a hex encoded uid
   * @return A UniqueIdMap if successful, null if it wasn't found
   */
  private final UniqueIdMap getMapFromStorage(final String id) {
    String qualifier = fromBytes(kind) + "_map";
    try {
      final byte[] raw = storage.getValue(StringtoID(id), NAME_FAMILY,
          toBytes(qualifier));
      if (raw == null){
        LOG.trace(String.format("Couldn't find %s UID [%s]'s map in storage",
            fromBytes(kind), id));
        return null;
      }
      UniqueIdMap map = new UniqueIdMap(id);
      if (!map.deserialize(raw))
        return null;
      // fix missing uid
      if (map.getUid() == null)
        map.setUid(id);
      
      // cache
      this.uid_map.put(id, map);
      return map;
    } catch (TsdbStorageException ex) {
      LOG.error(String.format("Storage Exception [%s]", ex.getMessage()));
      return null;
    } catch (Exception e) {
      LOG.error(String.format("Unhandled exception occurred [%s]",
          e.getMessage()));
      return null;
    }
  }

  /** The start row to scan on empty search strings. `!' = first ASCII char. */
  private static final byte[] START_ROW = new byte[] { '!' };

  /** The end row to scan on empty search strings. `~' = last ASCII char. */
  private static final byte[] END_ROW = new byte[] { '~' };

  /**
   * Creates a scanner that scans the right range of rows for suggestions.
   */
  private TsdbScanner getSuggestScanner(final String search) {
    final byte[] start_row;
    final byte[] end_row;
    if (search.isEmpty()) {
      start_row = START_ROW;
      end_row = END_ROW;
    } else {
      start_row = toBytes(search);
      end_row = Arrays.copyOf(start_row, start_row.length);
      end_row[start_row.length - 1]++;
    }
    final TsdbScanner scanner = new TsdbScanner(start_row, end_row, table);
    scanner.setFamily(ID_FAMILY);
    scanner.setQualifier(kind);
    scanner.setMaxRows(MAX_SUGGESTIONS);
    return storage.openScanner(scanner);
  }

  /**
   * Creates a scanner that loads the whole set of UIDs for this particular type
   * @return A scanner to use in an actual scan
   */
  private TsdbScanner getFullScanner(final ScanType type) {
    final byte[] start_row;
    final byte[] end_row;
    if (type == ScanType.MAP || type == ScanType.META){
      start_row = new byte[] {0, 0, 0};
      end_row = new byte[] {127, 127, 127};
    }else{
      start_row = START_ROW;
      end_row = END_ROW;
    }
    final TsdbScanner scanner = new TsdbScanner(start_row, end_row, table);
    switch (type){
    case NAME:
      scanner.setFamily(ID_FAMILY);
      scanner.setQualifier(kind);
      break;
    case MAP:
      scanner.setFamily(NAME_FAMILY);
      scanner.setQualifier(toBytes(fromBytes(kind) + "_map"));
      break;
    case META:
      scanner.setFamily(NAME_FAMILY);
      scanner.setQualifier(toBytes(fromBytes(kind) + "_meta"));
    }
    return storage.openScanner(scanner);
  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  private static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    return "UniqueId(" + fromBytes(table) + ", " + kind() + ", " + idWidth
        + ")";
  }

}

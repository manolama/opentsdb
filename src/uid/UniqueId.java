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
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.JSON;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaDataCache;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.search.SearchQuery.SearchOperator;
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
public final class UniqueId {

  private static final Logger LOG = LoggerFactory.getLogger(UniqueId.class);

  private enum ScanType {
    NAME,
    MAP,
    META,
    ID
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
  private long last_full_meta_load = 0;

  /** Metadata associated with this UID */
  private final MetaDataCache metadata;

  private final ReentrantLock locker = new ReentrantLock();
  private long last_uid = 0;
  
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
    this.metadata = new MetaDataCache(client, table, false, kind);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    return "UniqueId(" + fromBytes(table) + ", " + kind() + ", " + idWidth
        + ")";
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

  /** The number of times we avoided reading from HBase thanks to the cache. */
  public int cacheHits() {
    return cacheHits;
  }

  /** The number of times we had to read from HBase and populate the cache. */
  public int cacheMisses() {
    return cacheMisses;
  }

  /** Returns the number of elements stored in the internal cache. */
  public int cacheSizeName() {
    return nameCache.size();
  }
  
  public int cacheSizeID() {
    return idCache.size();
  }
  
  public int cacheSizeMeta(){
    return this.metadata.size();
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
      name = getNameFromStorage(id);
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

  private String getNameFromStorage(final byte[] id) throws HBaseException {
//    LOG.trace(String.format("ID as string [%s] from byte [%s] kind [%s] fam [%s]", 
//        IDtoString(id), Arrays.toString(id), fromBytes(kind), fromBytes(NAME_FAMILY)));
    final byte[] name = storage.getValue(id, NAME_FAMILY, kind);
    return name == null ? null : fromBytes(name);
  }

  private void addNameToCache(final byte[] id, final String name) {
    final String key = fromBytes(id).intern();
    String found = idCache.get(key);
    if (found == null) {
      found = idCache.putIfAbsent(key, name);
    }
    if (found != null && !found.equals(name)) {
      throw new IllegalStateException("id=" + IDtoString(id) + " => name="
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
      try{
        addNameToCache(id, name);
      }catch (IllegalStateException ise){
        // todo - if the name already exists, we need to fix it
        LOG.error(ise.getMessage());
      }
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

  public GeneralMeta getGeneralMeta(final byte[] id, final boolean cache) {
    try{
      if (this.bad_meta_ids.contains(id)){
        LOG.trace("ID [" + IDtoString(id) + "] was in the bad list");
        return null;
      }
      
      GeneralMeta meta = this.metadata.getGeneralMeta(id, cache);
      if (meta.getName().length() < 1 && meta.getCreated() < 1){
        LOG.trace(String.format("Didn't find %s metadata for UID [%s]",
            fromBytes(kind), IDtoString(id)));
        String name = this.getName(id);
        if (name == null || name.isEmpty()){
          LOG.error(String.format("%s UID [%s] does not exist in storage", fromBytes(kind), 
              IDtoString(id)));
          this.bad_meta_ids.add(id);
          return null;
        }
        //LOG.trace("New Meta " + name);
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

  public GeneralMeta putMeta(final GeneralMeta meta, final boolean flush) {
    return this.metadata.putMeta(meta, flush);
  }
  
  public boolean haveMeta(final String uid){
    return this.metadata.haveMeta(uid);
  }
  
  /**
   * Call this to fix a specific UID forward mapping
   * 
   * NOTE: this will fetch values directly from storage
   * @param uid
   * @return
   */
//  public boolean fixStorage(final byte[] uid, final boolean recursing){
//    
//    final String name_from_id = this.getNameFromStorage(uid);
//    byte[] id_from_name = null;
//    if (name_from_id != null)
//      id_from_name = this.getIdFromStorage(name_from_id);
//    
//    // we have a forward mapping error, so see if we can find a matching ID with a name
//    if (name_from_id == null && id_from_name != null){
//      
//    }
//  }
  
  /**
   * Attempts to fix a reverse mapping error
   * @param name
   * @return
   */
//  public boolean fixStorage(final String name, final boolean recursing){
//    byte[] id_from_name = this.getIdFromStorage(name);
//    String name_from_id = null;
//    if (id_from_name != null)
//      name_from_id = this.getNameFromStorage(id_from_name);
//    
//    List<byte[]> uids = this.searchStorageForName(name);
//    
//    // if the name wasn't found, scan for it
//    if (name_from_id == null && id_from_name == null){
//      if (uids.size() < 1){
//        LOG.error(String.format("Unable to locate %s name [%s] in storage", fromBytes(kind), name));
//        return false;
//      }
//      LOG.debug(String.format("Found %s [%d] UIDs matching name [%s]", fromBytes(kind), 
//          uids.size(), name));
//      
//      // we have one or more IDs! so see what their name is
//      for (byte[] uid : uids){
//        final String store_name = this.getNameFromStorage(uid);
//        if (store_name == null){
//          LOG.warn(String.format("Unable to find matching name in storage for %s UID [%s]", 
//              fromBytes(kind), IDtoString(uid)));
//          
//          // WARNING - This could potentially cause an infinite recursion!
//          if (!recursing)
//            this.fixStorage(uid, true);
//        }
//        
//        if (store_name.compareTo(name) == 0){
//          LOG.info(String.format("Matched %s UID [%s] to name [%s]", fromBytes(kind), 
//              IDtoString(uid), store_name));
//          
//          // store forward map
//          if (!putFix(toBytes(store_name), uid, ID_FAMILY)){
//            LOG.warn(String.format("Unable to store %s UID [%s] forward map for [%s]", 
//                fromBytes(kind), IDtoString(uid), store_name));
//          }else{
//            LOG.info(String.format("Successfully stored %s UID [%s] forward map for [%s]", 
//                fromBytes(kind), IDtoString(uid), store_name));
//            this.nameCache.put(store_name, uid);
//            this.idCache.put(fromBytes(uid), store_name);
//          }
//        }
//      }
//      return true;
//    }
//    
//    // we have a forward mapping error, so see if we can find a matching ID with a name
//    if (name_from_id == null && id_from_name != null){
//      LOG.warn(String.format("%s UID [%s] was missing forward mapping for [%s]", fromBytes(kind),
//          IDtoString(id_from_name), name));
//      
//      // store forward map
//      if (!putFix(toBytes(name), id_from_name, ID_FAMILY)){
//        LOG.warn(String.format("Unable to store %s UID [%s] forward map for [%s]", 
//            fromBytes(kind), IDtoString(id_from_name), name));
//      }else{
//        LOG.info(String.format("Successfully stored %s UID [%s] forward map for [%s]", 
//            fromBytes(kind), IDtoString(id_from_name), name));
//        this.nameCache.put(name, id_from_name);
//        this.idCache.put(fromBytes(id_from_name), name);
//      }
//      
//      
//    }
//    
//    // fix a name miss-match
//    if (name_from_id != null && name_from_id.compareTo(name) != 0){
//      
//    }
//  }  

  public void flushMeta(){
    this.metadata.flush();
  }

  public byte[] getOrCreateId(String name) throws HBaseException {
    try {
      return getId(name);
    } catch (NoSuchUniqueName e) {
      LOG.info("Creating an ID for kind='" + kind() + "' name='" + name
          + '\'');
    }
    
    short attempt = MAX_ATTEMPTS_ASSIGN_ID;
    TsdbStorageException hbe = null;
    long id = 0; // The ID.
    byte row[] = null; // The same ID, as a byte array.
    
    while (attempt-- > 0) {
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
          final byte[] uid = getId(name);
          LOG.warn("Race condition, found ID for kind='" + kind() + "' name='"
              + name + '\'');
          return uid;
        } catch (NoSuchUniqueName e) {
          // OK, the row still doesn't exist, let's create it now.
        }
        
        // since external locking can take a while, we need to use an internal lock when
        // creating the ID
        try{
          if (!this.locker.tryLock(250, TimeUnit.MILLISECONDS)){
            LOG.error("Unable to acquire lock in 250 ms");
            continue;
          }
          // Assign an ID.
          try {
            // We want to send an ICV with our explicit RowLock, but HBase's RPC
            // interface doesn't expose this interface. Since an ICV would
            // attempt to lock the row again, and we already locked it, we can't
            // use ICV here, we have to do it manually while we hold the RowLock.
            // To be fixed by HBASE-2292.
            { // HACK HACK HACK
              this.last_uid++;
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
                
                // NOTE: Older versions of HBase may screw up if you are incrementing the 
                // UID very fast in that it may return a cached or stale value from a different
                // region. The solution is to use a local counter as a sanity check.
                if (Bytes.getLong(row) > this.last_uid){
                  LOG.debug("Storage UID [" + Bytes.getLong(row) + 
                      "] was greater than local [" + this.last_uid + "]");
                  this.last_uid = Bytes.getLong(row);
                }else if (Bytes.getLong(row) < this.last_uid){
                  LOG.warn("Storage UID [" + Bytes.getLong(row) + 
                      "] was less than local [" + this.last_uid + "]");
                  row = Bytes.fromLong(this.last_uid);
                }
                
              }
              // final PutRequest update_maxid = new PutRequest(
              // table, MAXID_ROW, ID_FAMILY, kind, row, lock);
              // hbasePutWithRetry(update_maxid, MAX_ATTEMPTS_PUT,
              // INITIAL_EXP_BACKOFF_DELAY);
              storage.putWithRetry(MAXID_ROW, ID_FAMILY, kind, row, 0, lock);
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
          addIdToCache(name, row);
          addNameToCache(row, name);

        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }finally{
          this.locker.unlock();
        }
        
        // Create the reverse mapping first, so that if we die before creating
        // the forward mapping we don't run the risk of "publishing" a
        // partially assigned ID. The reverse mapping on its own is harmless
        // but the forward mapping without reverse mapping is bad.
        try {
          // final PutRequest reverse_mapping = new PutRequest(
          // table, row, NAME_FAMILY, kind, toBytes(name));
          // hbasePutWithRetry(reverse_mapping, MAX_ATTEMPTS_PUT,
          // INITIAL_EXP_BACKOFF_DELAY);
          storage.putWithRetry(row, NAME_FAMILY, kind, toBytes(name), 0);
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
          storage.putWithRetry(toBytes(name), ID_FAMILY, kind, row, 0);
        } catch (TsdbStorageException e) {
          LOG.error("Failed to Put forward mapping!  ID leaked: " + id, e);
          hbe = e;
          continue;
        }
        
        // todo - CL This could be slowing us way the hell down
        // if we are a tag name or metrics ID, add a meta entry
        GeneralMeta meta = new GeneralMeta();
        meta.setUID(UniqueId.IDtoString(row));
        meta.setCreated(System.currentTimeMillis() / 1000L);
        meta.setName(name);
        this.metadata.putMeta(meta, false);

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
 
  /**
   * Retrieves the entire list of entries in the cache as a sorted map for
   * display in the HTTP API
   * @return A sorted map of UIDs and their numeric IDs
   */
  public final SortedMap<String, byte[]> getMap() {
    loadAllUIDs();
    final TreeMap<String, byte[]> sorted = new TreeMap<String, byte[]>();
    for (Map.Entry<String, byte[]> entry : this.nameCache.entrySet()) {
      // TODO fix this, it's not returning the actual numeric ID
      if (entry.getValue() == null || entry.getValue().length != 8)
        sorted.put(entry.getKey(), new byte[] {0});
      else
        sorted.put(entry.getKey(), entry.getValue());
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
      storage.putWithRetry(row, NAME_FAMILY, kind, newnameb, 0);
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
      storage.putWithRetry(newnameb, NAME_FAMILY, kind, row, 0);
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

  public final ArrayList<byte[]> matchRegex(final Pattern reg){
    this.loadAllUIDs();
    
    ArrayList<byte[]> matches = new ArrayList<byte[]>();
    for (Map.Entry<String, byte[]> entry: this.nameCache.entrySet()){
      if (reg.matcher(entry.getKey()).find())
        matches.add(entry.getValue());
    }
    return matches;
  }
  
// PRIVATES -----------------------------------
  
  private final boolean putFix(final byte[] row, final byte[] value, final byte[] family){
    short attempt = 3;
    Object lock = null;
    
    try{
      while(attempt-- > 0){
        // first, we need to lock the row for exclusive access on the set
        try {
          lock = this.storage.getRowLock(row);          
          if (lock == null) {  // Should not happen.
            LOG.error("Received null for row lock");
            continue;
          }

          this.storage.putWithRetry(row, family, kind, value, 0, lock)
              .joinUninterruptibly();

          // do NOT forget to unlock
          this.storage.releaseRowLock(lock);
          return true;
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
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      return false;
    } catch (Exception e){
      e.printStackTrace();
      return false;
    }finally {
      LOG.trace("Releasing lock");
      this.storage.releaseRowLock(lock);
    }
    return false;
  }
  
  /**
   * Scans through storage for an UID matching the given name
   */
  private final List<byte[]> searchStorageForName(final String name){
    final TsdbScanner scanner = this.getFullScanner(ScanType.NAME);
    List<byte[]> uids = new ArrayList<byte[]>();
    
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
          final String uid_name = fromBytes(row.get(0).value());
          
          if (name.compareTo(uid_name) == 0){
            LOG.debug(String.format("Matched name [%s] to %s UID [%s]", name, 
                fromBytes(kind), IDtoString(key)));
            uids.add(key);
          }
        }
      }
    } catch (TsdbStorageException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
    return uids;
  }
  
  /**
   * Scans through storage for a name matching the given ID
   * @param uid
   * @return
   */
  private final List<String> searchStorageForID(final byte[] uid){
    final TsdbScanner scanner = this.getFullScanner(ScanType.ID);
    List<String> names = new ArrayList<String>();
    
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
          final String uid_name = fromBytes(row.get(0).key());
          final byte[] name_uid = row.get(0).value();
          
          if (Arrays.equals(uid, name_uid)){
            LOG.debug(String.format("Matched UID [%s] to %s name [%s]", IDtoString(name_uid), 
                fromBytes(kind), uid_name));
            names.add(uid_name);
          }
        }
      }
    } catch (TsdbStorageException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
    return names;
  }
  
  /**
   * Scans the ID AND the Name families for the given type to find out what
   * the actual max ID is that has been recorded
   * @return NULL if invalid, a UID for the local kind if found
   */
  private final byte[] getMaxUsedID(){
    
    long max_id = 0;
    
    try{
      TsdbScanner scanner = new TsdbScanner(new byte[] {0, 0, 0}, 
          new byte[] {127, 127, 127}, table);
      scanner = this.storage.openScanner(scanner);
      
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (final KeyValue kv : row) {  
            final String family = fromBytes(kv.family());

            // skip cells that aren't of this kind
            if (!Arrays.equals(kind, kv.qualifier()))
              continue;
            
            long current_id = 0;
            if (family.compareTo("id") == 0){
              current_id = Bytes.getLong(kv.value());
            }else if (family.compareTo("name") == 0){
              current_id = Bytes.getLong(kv.key());
            }else{
              LOG.warn(String.format("Unrecognized column family [%s]", family));
              continue;
            }
            
            if (current_id > max_id)
              max_id = current_id;
          }
        }
      }     
    }catch (TsdbStorageException e) {
      throw e;
    } catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
    return max_id > 0 ? Bytes.fromLong(max_id) : null;
  }

  private final boolean fixMaxID(final byte[] max){
    if (max == null){
      LOG.error("Given max ID was null");
      return false;
    }
    
    short attempt = 3;
    Object lock = null;
    
    try{
      while(attempt-- > 0){
        // first, we need to lock the row for exclusive access on the set
        try {
          lock = this.storage.getRowLock(MAXID_ROW);          
          if (lock == null) {  // Should not happen.
            LOG.error("Received null for row lock");
            continue;
          }

          this.storage.putWithRetry(MAXID_ROW, ID_FAMILY, kind, max, 0, lock)
              .joinUninterruptibly();

          LOG.info(String.format("Updated MAX ID for %s to [%d]", fromBytes(kind), Bytes.getLong(max)));
          // do NOT forget to unlock
          this.storage.releaseRowLock(lock);
          return true;
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
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      return false;
    } catch (Exception e){
      e.printStackTrace();
      return false;
    }finally {
      LOG.trace("Releasing lock");
      this.storage.releaseRowLock(lock);
    }
    return false;
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
      break;
    case ID:
      scanner.setFamily(NAME_FAMILY);
      scanner.setQualifier(kind);
    }
    return storage.openScanner(scanner);
  }

  private static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  private static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }
}

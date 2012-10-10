package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaData;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;

import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the timeseries uids and provides thread safety since
 * the SET classes are not thread-safe
 */
public class TimeseriesUID {
  private static final Logger LOG = LoggerFactory.getLogger(TimeseriesUID.class);
  private final Map<String, HashSet<String>> ts_uids;
  
  // these locks should only be used for the hashes
  private final Set<Integer> ts_uid_hashes;
  private final ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
  private final Lock read_lock  = locker.readLock();
  private final Lock write_lock = locker.writeLock();
  
  
  private final TsdbStore uid_storage;
  
  private final Set<String> tsuid_queue;
  
  public TimeseriesUID(final TsdbStore store){
    ts_uids = new HashMap<String, HashSet<String>>();
    ts_uid_hashes = new HashSet<Integer>();
    tsuid_queue = new HashSet<String>();
    this.uid_storage = store;
  }
  
  public final boolean contains(final String uid){
    this.read_lock.lock();
    try{
      return this.ts_uid_hashes.contains(uid.hashCode());
    }finally{
      this.read_lock.unlock();
    }
  }
  
  public final void add(final String uid){
    this.write_lock.lock();
    try{
      final String metric = getMetric(uid);
      //LOG.debug(String.format("Processing tsuid for metric [%s]", metric));
      if (!this.ts_uids.containsKey(metric)){
        HashSet<String> uids = new HashSet<String>();
        uids.add(uid);
        this.ts_uids.put(metric, uids);
      }else{
        this.ts_uids.get(metric).add(uid);
      }
      this.ts_uid_hashes.add(uid.hashCode());
    }finally{
      this.write_lock.unlock();
    }
  }
  
  public final int stringSize(){
//    this.read_lock.lock();
//    try{
      return this.ts_uids.size();
//    }finally{
//      this.read_lock.unlock();
//    }
  }
  
  public final int intSize(){
    this.read_lock.lock();
    try{
      return this.ts_uid_hashes.size();
    }finally{
      this.read_lock.unlock();
    }
  }
  
  /**
   * Writes any changes in the tsuids to storage
   */
  public final boolean flush(){
//    this.write_lock.lock();
//    try{
      Iterator<Entry<String, HashSet<String>>> it = this.ts_uids.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, HashSet<String>> uids = (Map.Entry<String, HashSet<String>>)it.next();
        this.flushSet(uids.getKey());
        it.remove();
      }
//    }finally{
//      this.write_lock.unlock();
//    }
    LOG.debug("Completed flush of TSUIDs");  
    return true;
  }
  
  public final boolean processMaps(final UniqueId metrics, final UniqueId tag_names, 
      final UniqueId tag_values, final MetaData timeseries_meta, final boolean update_meta){
    try{
      
      Iterator<String> queue_iterator = this.tsuid_queue.iterator();
      while (queue_iterator.hasNext()){
        final String tsuid = queue_iterator.next();
        
        // update maps
        String metric = tsuid.substring(0, 6);
        List<byte[]> pairs = getTagPairsFromTSUID(tsuid, (short)3, (short)3);
        List<byte[]> tagks = getTagksFromTagPairs(pairs, (short)3);
        List<byte[]> tagvs = getTagvsFromTagPairs(pairs, (short)3);
        final long timestamp = System.currentTimeMillis() / 1000;
        LOG.trace(String.format("Processing new TSUID [%s]", tsuid));
        
        // metric            
        for (byte[] p : pairs)
          metrics.putMap(metric, UniqueId.IDtoString(p), "tags");
        
        // tagk
        for (byte[] tagk : tagks){
          for (byte[] p : pairs)
            tag_names.putMap(UniqueId.IDtoString(tagk), UniqueId.IDtoString(p), "tags");
          
          // meta data
          if (update_meta){
            GeneralMeta meta = tag_names.getGeneralMeta(tagk);
            if (meta == null){
              meta = new GeneralMeta(tagk);
              meta.setName(tag_names.getName(tagk));
              meta.setCreated(timestamp);
              tag_names.putMeta(meta, false);
            }else if (meta.getCreated() < 1){
              meta.setCreated(timestamp);
              tag_names.putMeta(meta, false);
            }
          }
        }
        
        // tagv
        for (byte[] tagv : tagvs){
          for (byte[] p : pairs)
            tag_values.putMap(UniqueId.IDtoString(tagv), UniqueId.IDtoString(p), "tags");
          
          // meta data
          if (update_meta){
            GeneralMeta meta = tag_values.getGeneralMeta(tagv);
            if (meta == null){
              meta = new GeneralMeta(tagv);
              meta.setName(tag_values.getName(tagv));
              meta.setCreated(timestamp);
              tag_values.putMeta(meta, false);
            }else if (meta.getCreated() < 1){
              meta.setCreated(timestamp);
              tag_values.putMeta(meta, false);
            }
          }
        }
        
        if (update_meta){
          // metric meta data
          GeneralMeta meta = metrics.getGeneralMeta(UniqueId.StringtoID(metric));
          if (meta == null){
            meta = new GeneralMeta(UniqueId.StringtoID(metric));
            meta.setName(metrics.getName(UniqueId.StringtoID(metric)));
            meta.setCreated(timestamp);
            metrics.putMeta(meta, false);
          }else if (meta.getCreated() < 1){
            meta.setCreated(timestamp);
            metrics.putMeta(meta, false);
          }
          
          // TS meta data
          TimeSeriesMeta tsmd = timeseries_meta.getTimeSeriesMeta(UniqueId.StringtoID(tsuid));
          if (tsmd == null){
            tsmd = new TimeSeriesMeta(UniqueId.StringtoID(tsuid));
            tsmd.setFirstReceived(timestamp);
            timeseries_meta.putMeta(tsmd, false);
          }else if (tsmd.getFirstReceived() < 1){
            tsmd.setFirstReceived(timestamp);
            timeseries_meta.putMeta(tsmd, false);
          }
        }
        
        // delete the entry now that we're done
        queue_iterator.remove();
      }
      return true;
    }catch (NullPointerException npe){
      npe.printStackTrace();
      return false;
    }
  }
  
  /**
   * Searches through all of the ts_uids in the ID table and determines if any
   * contain the given tags
   * @param tags
   * @param metric_width
   * @return
   */
  public final Set<String> matchTSUIDs(final TSDB tsdb, final Set<String> tags, 
      final short metric_width, final boolean load_all){
    
    HashSet<String> matches = new HashSet<String>();
    
    final byte[] start_row= new byte[] {0, 0, 0};
    final byte[] end_row = new byte[] {127, 127, 127};
    TsdbScanner scanner = new TsdbScanner(start_row, end_row, this.uid_storage.getTable());
    scanner.setFamily(TsdbStore.toBytes("id"));
    scanner.setQualifier(TsdbStore.toBytes("ts_uids"));
    scanner = this.uid_storage.openScanner(scanner);
    
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      JSON codec = new JSON(new HashSet<String>());
      while ((rows = this.uid_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.size() != 1) {
            LOG.error("WTF shouldn't happen!  Scanner " + scanner + " returned"
                + " a row that doesn't have exactly 1 KeyValue: " + row);
            if (row.isEmpty()) {
              continue;
            }
          }
          final String metric = tsdb.metrics.getName(row.get(0).key());
          if (!codec.parseObject(row.get(0).value())){
            LOG.warn(String.format("Unable to parse tsuids for metric [%s]", metric));
            continue;
          }
          
          HashSet<String> uids = (HashSet<String>)codec.getObject();
          if (uids == null){
            LOG.debug(String.format("No timeseries UIDs for metric [%s]", metric));
            continue;
          }
          
          long count = 0;
          if (load_all){
            matches.addAll(uids);
            count = uids.size();
          }else{
            // todo(CL) - there MUST be a better way. This could take ages
            for (String pair : tags){
              for (String tsuid : uids){
                // need to start AFTER the metric
                if (tsuid.substring(metric_width*2).contains(pair)){
                  matches.add(tsuid);
                  count++;
                }
              }
            }
          }
          LOG.trace(String.format("Matched [%d] timeseries UIDs for metric [%s]", count, 
              metric));
        }
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
    return matches;
  }
  
  /**
  * Extracts a list of tagk/tagv pairs from a tsuid
  * @param tsuid The tsuid to parse
  * @param metric_width The width of the metric tag in bytes
  * @param tag_width The width of the tags in bytes
  * @return A list of tagk/tagv pairs as a single byte array
  */
  public static List<byte[]> getTagPairsFromTSUID(final String tsuid,
     final short metric_width, final short tag_width) {
   List<byte[]> tags = new ArrayList<byte[]>();
   for (int i = metric_width * 2; i < tsuid.length(); i+= (tag_width * 2) * 2) {
     if (i + (tag_width * 2) * 2 > tsuid.length()){
       LOG.warn(String.format("Error parsing tsuid [%s], improper length", tsuid));
       return null;
     }
     tags.add(UniqueId.StringtoID(tsuid.substring(i, i + (tag_width * 2) * 2)));
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

  // PRIVATES ---------------------------------------  

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
  
  private final String getMetric(final String uid){
    if (uid.length() <= 6){
      LOG.error("UID was less than or equal to metric length");
      return null;
    }
    
    return uid.substring(0, 6);
  }
  
  
  public final boolean flushSet(final String metric_uid){
    if (!this.ts_uids.containsKey(metric_uid)){
      LOG.error(String.format("Metric uid [%s] not found in hash table", metric_uid));
      return false;
    }
    HashSet<String> metric_uids = this.ts_uids.get(metric_uid);
    if (metric_uids.size() < 1){
      LOG.warn(String.format("Uids for metric [%s] were empty", metric_uid));
      return false;
    }
    
    short attempt = 3;
    Object lock = null;
    byte[] uid = UniqueId.StringtoID(metric_uid);
    try{
      while(attempt-- > 0){
        LOG.debug(String.format("Attempting to sync Timeseries UIDs on metric [%s]", 
            metric_uid));
        // first, we need to lock the row for exclusive access on the set
        try {
          lock = this.uid_storage.getRowLock(uid);          
          if (lock == null) {  // Should not happen.
            LOG.error("Received null for row lock");
            continue;
          }
          LOG.debug(String.format("Successfully locked UID row [%s]", metric_uid));
          
          HashSet<String> temp_uids = new HashSet<String>();
          JSON codec = new JSON(new HashSet<String>());
          
          // get the current value from storage so we don't overwrite other TSDs changes
          byte[] uids = uid_storage.getValue(uid, TsdbStore.toBytes("id"), 
              TsdbStore.toBytes("ts_uids"), lock);
          if (uids == null){
            LOG.warn(String.format("Timeseries UID list was not found in the storage system for metric [%s]",
                metric_uid));
          }else{
            if (!codec.parseObject(uids)){
              LOG.error(String.format("Unable to parse Timeseries UID list from storage for metric [%s]",
                  metric_uid));
              return false;
            }
            temp_uids = (HashSet<String>)codec.getObject();
            if (temp_uids.size() > 0)
              LOG.debug(String.format("Successfully loaded Timeseries UID list from the storage system [%d] tsuids",
                temp_uids.size()));
            
            // now we compare the newly loaded list and the old one, if there are any differences,
            // we need to update storage
            if (metric_uids.equals(temp_uids)){
              LOG.debug("No changes from stored data");
              return true;
            }
          }          
          
          // there was a difference so check for new tsuids that need maps
          Iterator<String> metric_iterator = metric_uids.iterator();
          while (metric_iterator.hasNext()){
            final String muid = metric_iterator.next();
            if (!temp_uids.contains(muid)){
              LOG.trace(String.format("Detected new TSUID [%s]", muid));
              this.tsuid_queue.add(muid);
            }
          }
          
          // then merge and put
          int old_size = temp_uids.size();
          temp_uids.addAll(metric_uids);
          if (temp_uids.size() < 1){
            LOG.debug("No UIDs to store");
            return true;
          }
          LOG.trace(String.format("TS UIDs for [%s] requires updating, old size [%d], new [%d]",
              metric_uid, old_size, temp_uids.size()));            
          
          codec = new JSON(temp_uids);
          this.uid_storage.putWithRetry(uid, TsdbStore.toBytes("id"), 
              TsdbStore.toBytes("ts_uids"), codec.getJsonBytes(), lock)
              .joinUninterruptibly();
          LOG.info(String.format("Successfully updated Timeseries UIDs for [%s] in storage", metric_uid));
          // do NOT forget to unlock
          this.uid_storage.releaseRowLock(lock);
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
      this.uid_storage.releaseRowLock(lock);
    }
    return true;
  }
  
  private final HashSet<String> getTSUIDs(final String metric_uid){
    byte[] uid = UniqueId.StringtoID(metric_uid);
    LOG.debug(String.format("Attempting to fetch Timeseries UIDs for metric [%s]", 
        metric_uid));
    // first, we need to lock the row for exclusive access on the set
    try {

      HashSet<String> uids = new HashSet<String>();
      JSON codec = new JSON(uids);
      
      // get the current value from storage so we don't overwrite other TSDs changes
      byte[] storage_uids = uid_storage.getValue(uid, TsdbStore.toBytes("id"), 
          TsdbStore.toBytes("ts_uids"));
      if (storage_uids == null){
        LOG.warn(String.format("Timeseries UID list was not found in storage for metric [%s]",
            metric_uid));
      }else{
        if (!codec.parseObject(storage_uids)){
          LOG.error(String.format("Unable to parse Timeseries UID list from storage for metric [%s]",
              metric_uid));
          return null;
        }
        uids = (HashSet<String>)codec.getObject();
        if (uids.size() > 0)
          LOG.debug(String.format("Successfully loaded [%d] Timeseries UIDs from metric [%s]",
              uids.size(), metric_uid));
        
        return uids;
      }          
    }catch (TsdbStorageException tex){
      LOG.warn(String.format("Exception from storage [%s]", tex.getMessage()));
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    } catch (Exception e){
      e.printStackTrace();
    }
    return null;
  }
}

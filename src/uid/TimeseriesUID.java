package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.ontopia.utils.CompactHashSet;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.MetaDataCache;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchIndexer;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;

import org.apache.lucene.document.Document;
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
  
  // these locks should only be used for the hashes
  
  // Take a look at the Apache.math OpenIntToDoubleHashMap
  // looks like the Apache one ate up moe ram than the compact one
  private final CompactHashSet<Integer> ts_uid_hashes;
  private final ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
  private final Lock read_lock  = locker.readLock();
  private final Lock write_lock = locker.writeLock();
  
  private final TsdbStore uid_storage;
  
  private final Set<String> tsuid_queue;
  private final AtomicLong new_tsuids = new AtomicLong();
  
  public TimeseriesUID(final TsdbStore store){
    ts_uid_hashes = new CompactHashSet<Integer>();
    tsuid_queue = new HashSet<String>();
    this.uid_storage = store;
  }
  
  public final boolean contains(final String uid){
    try{
      if (this.read_lock.tryLock(100, TimeUnit.MILLISECONDS)){
        return this.ts_uid_hashes.contains(uid.hashCode());
      }else{
        LOG.warn(String.format("Failed to acquire lock for TSUID [%s]", uid));
        return false;
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      this.read_lock.unlock();
    }
    LOG.warn(String.format("Exception while trying to acquire lock for TSUID [%s]", uid));
    return false;
  }
  
  public final void add(final String uid){
    try{
      if (this.write_lock.tryLock(100, TimeUnit.MILLISECONDS)){
        this.tsuid_queue.add(uid);
        this.ts_uid_hashes.add(uid.hashCode());
      }else{
        LOG.warn(String.format("Failed to acquire lock for tsuid [%s]", uid));
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      this.write_lock.unlock();
    }
  }
  
  public final int queueSize(){
    return this.tsuid_queue.size();
}
  
  public final int hashSize(){
//    this.read_lock.lock();
//    try{
      return this.ts_uid_hashes.size();
//    }finally{
//      this.read_lock.unlock();
//    }
  }

  public final boolean processNewMeta(final UniqueId metrics, final UniqueId tag_names, 
      final UniqueId tag_values, final MetaDataCache timeseries_meta){
    try{
      if (this.tsuid_queue.size() < 1){
        LOG.trace("No new TSUIDs to process");
        return true;
      }
      
      // lock and copy queue
      Set<String> local_queue = null;
      try{
        if (this.write_lock.tryLock(250, TimeUnit.MILLISECONDS)){
          try{
            local_queue = new HashSet<String>(this.tsuid_queue);
            this.tsuid_queue.clear();
          }finally {
            this.write_lock.unlock();
          }
        }else{
          LOG.error("Unable to acquire lock on queue");
          return false;
        }
      }catch (Exception e){
        e.printStackTrace();
      }

      LOG.debug(String.format("Processing [%s] new TSUIDs", local_queue.size()));
      
      long count = 0;
      Iterator<String> queue_iterator = local_queue.iterator();
      while (queue_iterator.hasNext()){
        final String tsuid = queue_iterator.next();
        // get meta
        if (timeseries_meta.haveMeta(tsuid)){
          LOG.trace(String.format("Already have meta for TSUID [%s]", tsuid));
          continue;
        }

        LOG.trace(String.format("Processing new TSUID [%s]", tsuid));
        TimeSeriesMeta tsmd = new TimeSeriesMeta(UniqueId.StringtoID(tsuid));
        tsmd.setCreated(System.currentTimeMillis() / 1000);
        timeseries_meta.putMeta(tsmd, true);
        this.new_tsuids.incrementAndGet();

        count++;
      }

      LOG.info(String.format("Processed [%d] new TSUIDs", count));
      return true;
    }catch (NullPointerException npe){
      npe.printStackTrace();
      return false;
    }catch (Exception e){
      e.printStackTrace();
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
   * Scans the entire tsdb-uid table for ts_meta data and loads the hash code into the 
   * hashes set for fast lookups on puts
   * @return
   */
  public final boolean loadAllHashes(){
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    //scanner.setStartRow(new byte[] {127, 127, 127});
    scanner.setFamily(TsdbStore.toBytes("name"));
    //scanner.setQualifier(TsdbStore.toBytes("ts_meta"));
    
    try {
      scanner = this.uid_storage.openScanner(scanner);
      LOG.info("Loading list of TSUIDs from UID table");
      long count=0;
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = this.uid_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            if (TsdbStore.fromBytes(cell.qualifier()).compareTo("ts_meta") == 0){
              final String uid = UniqueId.IDtoString(cell.key());
              this.ts_uid_hashes.add(uid.hashCode());
              count++;
            }
          }
        }
      }
      LOG.info(String.format("Loaded [%d] tsuid hashes", count));
      return true;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public static byte[] getMetric(final String tsuid){
    if (tsuid.length() < 3 * 2){
      LOG.error("TSUID was too short");
      return null;
    }
    
    return UniqueId.StringtoID(tsuid.substring(0, 6));
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

  public void collectStats(final StatsCollector collector) {
    collector.record("uid.tsuid.new", this.new_tsuids.get());
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
}

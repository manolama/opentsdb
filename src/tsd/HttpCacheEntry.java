package net.opentsdb.tsd;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HttpCacheEntry{
  private static final Logger LOG = LoggerFactory.getLogger(HttpCacheEntry.class);
  
  /** Hash of the query that generated this cache object */
  private int id = 0;
  /** Data representing the cache object in memory */
  private byte[] data = null;
  /** How much data is stored in the byte array */
  private int data_size = 0;
  /** Path to the file if file caching is enabled */
  private String file = "";
  /** Unix epoch timestamp when the cache object was created */
  private long created = 0;
  /** Unix epoch timestamp when the object should expire */
  private long expire_ts = 0;
  /** How long before the object should expire */
  private long expire = 0;
  /** True if we should not store any data in RAM and the file was persisted for us */
  private boolean file_only = false;
  /** Tracks how many times this object has been hit */
  private final AtomicInteger hits = new AtomicInteger();

  /**
   * Constructor generates a new entry object
   * @param key Query hash identifying this object
   * @param d Binary data to store
   * @param path Path to a file to store and/or read data
   * @param fileOnly True if the file was already created and shouldn't be stored in RAM
   * @param exp How long, in seconds, to keep the object in cache
   */
  public HttpCacheEntry(final int key, final byte[] d, final String path,
      final boolean fileOnly, final long exp) {
    id = key;
    data = d;
    if (d != null)
      data_size = d.length;
    file = path;
    file_only = fileOnly;
    created = System.currentTimeMillis() / 1000L;
    expire = exp;
    expire_ts = created + exp;
  }

  /**
   * Increments how many times this file was requested
   */
  public void incrementHits() {
    hits.incrementAndGet();
  }
  
  /**
   * Determines if this entry has expired according to the users settings.
   * @return True if this object is expired, false if not
   */
  public boolean hasExpired(){
    if ((System.currentTimeMillis() / 1000L) < expire_ts)
      return false;
    else
      return true;
  }
  
  /**
   * Generates a meta object (just hits and data size) 
   * @return A new HttpCacheEntryMeta object for sorting
   */
  public final HttpCacheEntryMeta getMeta(){
    return new HttpCacheEntryMeta(id, hits.get(), (data != null ? data.length : 0));
  }
  
  /**
   * Erases the data in RAM
   * @return False if the data was already empty, true if it was flushed
   */
  public final boolean flushData(){
    if (data == null){
      data_size = 0;
      return false;
    }
    data = null;
    data_size = 0;
    return true;
  }

  /**
   * Getter that returns the query hash
   * @return The Query hash
   */
  public final int getKey(){
    return id;
  }
  
  /**
   * Getter that returns the data
   * @return data or null if data isn't set
   */
  public final byte[] getData() {
    return data;
  }

  /**
   * Getter that returns the amount of data in the byte array
   * @return How much data is in the byte array, 0 if there isn't any data
   */
  public final int getDataSize(){
    return data_size;
  }
  
  /**
   * Getter returning the path to the cache file
   * @return Path to the cache file
   */
  public final String getFile() {
    return file;
  }
  
  /**
   * Getter returning the FileOnly flag
   * @return True or false
   */
  public boolean getFileOnly() {
    return file_only;
  }
  
  /**
   * Getter that returns how long the object should remain in cache
   * @return Time in seconds
   */
  public final long getExpire(){
    return expire;
  }
}

/**
 * This is a small class containing simple metadata about cached objects
 * such as the size and how many times it's been hit. It's used for creating
 * a sorted list and figuring out what to eject
 */
class HttpCacheEntryMeta implements Comparable<HttpCacheEntryMeta> {
  /** How many times this object has been hit */
  private int hits;
  /** How much data is in RAM */
  private int data_size;
  /** Query hash of the object */
  private int id;
  
  /**
   * Default constructor to initialize the object
   * @param i Query Hash ID
   * @param h Hits
   * @param ds Data size in RAM
   */
  public HttpCacheEntryMeta(final int i, final int h, final int ds){
    id = i;
    hits = h;
    data_size = ds;
  }
  
  /**
   * Comparator to determine what order the entries are sorted. It's based on
   * the hits and then the size of the data in cache with larger objects
   * getting preference
   * TODO re-order this so smaller objects get preference since it makes sense
   * to hit the disk for something big, but not tiny
   */
  public int compareTo(HttpCacheEntryMeta entry){
    // compare hits first
    if (entry.hits < hits)
      return -1;
    else if (entry.hits > hits)
      return 1;
    
    // hits are the same so compare size next
    if (entry.data_size ==  data_size)
      return 0;
    
    if (entry.data_size < data_size)
      return -1;
    else
      return 1;
  }
  
  /**
   * Getter returning the Query hash
   * @return The query hash
   */
  public final int getID(){
    return id;
  }
  
  /**
   * Getter returning the data size
   * @return The data size
   */
  public final int getDataSize(){
    return data_size;
  }
}

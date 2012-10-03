// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
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
package net.opentsdb.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single object in the cache system
 * 
 * If a file path is provided on creation, then the cache assumes that the file
 * has been created already (e.g. a GNUGraph plot) and won't load anything in
 * RAM.
 * 
 * Otherwise if an object is flushed to disk, then the file will be populated
 * 
 */
public class CacheEntry {
  private static final Logger LOG = LoggerFactory.getLogger(CacheEntry.class);

  /** Hash of the query that generated this cache object */
  private int id = 0;
  /** Data representing the cache object in memory */
  @JsonIgnore
  private byte[] data = null;
  /** How much data is stored in the byte array */
  private int data_size = 0;
  /** Path to the file if file caching is enabled */
  private String file = "";
  /** Size of the file on disk if flushed */
  private long file_size = 0;
  /** Set if the user provides a file path on creation */
  private boolean pre_existing_file = false;
  /** Unix epoch timestamp when the cache object was created */
  private long created = 0;
  /** Unix epoch timestamp when the object should expire */
  private long expire_ts = 0;
  /** How long, in seconds, after creation when the object should expire */
  private long expire = 300;
  /**
   * True if we should not store any data in RAM and the file was persisted for
   * us
   */
  private boolean file_only = false;
  /** Tracks how many times this object has been hit */
  private final AtomicInteger hits = new AtomicInteger();

  /**
   * Constructor that stores the data with defaults set
   * @param key Hash identifying the object
   * @param data The data representing the object
   */
  public CacheEntry(final int key, final byte[] data) {
    id = key;
    if (data != null) {
      this.data = data;
      data_size = this.data.length;
    }
    created = System.currentTimeMillis() / 1000L;
    expire_ts = created + expire;
  }

  public CacheEntry(final int key, final String existing_file) {
    id = key;
    this.file = existing_file;
    this.pre_existing_file = true;
    created = System.currentTimeMillis() / 1000L;
    expire_ts = created + expire;
    this.file_size = this.parseFileSize();
  }

  public CacheEntry(final int key, final String existing_file, final long exp) {
    id = key;
    this.file = existing_file;
    this.pre_existing_file = true;
    created = System.currentTimeMillis() / 1000L;
    this.expire = exp;
    expire_ts = created + expire;
    this.file_size = this.parseFileSize();
  }
  
  /**
   * Constructor generates a new entry object
   * @param key Query hash identifying this object
   * @param d Binary data to store
   * @param exp How long, in seconds, to keep the object in cache
   */
  public CacheEntry(final int key, final byte[] d, final long exp) {
    id = key;
    data = d;
    if (d != null)
      data_size = d.length;
    created = System.currentTimeMillis() / 1000L;
    expire = exp;
    expire_ts = created + exp;
  }

  /**
   * Constructor generates a new entry object
   * @param key Query hash identifying this object
   * @param d Binary data to store
   * @param fileOnly True if the data should be flushed to disk immediately
   * @param exp How long, in seconds, to keep the object in cache
   */
  public CacheEntry(final int key, final byte[] d, final boolean fileOnly,
      final long exp) {
    id = key;
    data = d;
    if (d != null)
      data_size = d.length;
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
  public boolean hasExpired() {
    if ((System.currentTimeMillis() / 1000L) < expire_ts)
      return false;
    return true;
  }

  /**
   * Generates a meta object (just hits and data size)
   * @return A new CacheEntryMeta object for sorting
   */
  @JsonIgnore
  public final CacheEntryMeta getMeta() {
    return new CacheEntryMeta(id, hits.get(), (data != null ? data.length : 0));
  }

  /**
   * Erases the data in RAM
   * @return False if the data was already empty, true if it was flushed
   */
  public final boolean flushData() {
    if (data == null) {
      data_size = 0;
      return false;
    }
    data = null;
    data_size = 0;
    return true;
  }

  /**
   * Writes the data to disk and removes it from RAM
   * @return True if the flush was successful, false if not
   */
  public final boolean flushToDisk() {
    if (data == null) {
      data_size = 0;
      return false;
    }

    File cache_file = new File(file);
    if (cache_file.exists()) {
      LOG.warn(String.format("Cache file [%s] exists, overwriting", this.file));
    }

    try {
      FileOutputStream fos = new FileOutputStream(file);
      fos.write(data);
      fos.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
    this.file_size = this.parseFileSize();
    data = null;
    data_size = 0;
    return true;
  }

  private final long parseFileSize() {
    try {
      File cache_file = new File(file);
      if (!cache_file.exists()) {
        LOG.error(String.format("Cache file [%s] does not exist", file));
        return 0;
      }
      return cache_file.length();
    } catch (Exception ex) {
      LOG.error(String.format("Error fetching file size [%s]", ex.getMessage()));
      return 0;
    }
  }

  // GETTERS AND SETTERS -------------------------------

  /**
   * Getter that returns the query hash
   * @return The Query hash
   */
  @JsonIgnore
  public final int getKey() {
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
   * Attempts to retrieve the file from disk
   * @return A byte array with the contents of the file if successful, null if
   *         there was an error
   */
  public final byte[] getFileData() {
    if (file == null || file.isEmpty())
      return null;

    try {
      InputStream stream = new FileInputStream(file);
      if (this.file_size > Integer.MAX_VALUE) {
        LOG.warn(String.format("File is too large for byte array [%d]",
            this.file_size));
        return null;
      }

      byte[] bytes = new byte[(int) this.file_size];
      int offset = 0;
      int read = 0;
      while (offset < bytes.length) {
        read = stream.read(bytes, offset, bytes.length - offset);
        if (read >= 0)
          offset += read;
        else
          break;
      }

      // Ensure all the bytes have been read in
      if (offset < bytes.length) {
        LOG.warn("Could not completely read file");
        return null;
      }

      // Close the input stream and return bytes
      stream.close();
      return bytes;
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Getter that returns the amount of data in the byte array
   * @return How much data is in the byte array, 0 if there isn't any data
   */
  public final int getDataSize() {
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
  public final long getExpire() {
    return expire;
  }

  /**
   * Getter that returns how many hits this object has had
   * @return Hits on this object as an int
   */
  public final int getHits() {
    return hits.get();
  }

  /**
   * Getter that returns the Unix epoch timestamp when this object should expire
   * @return Unix epoch timestamp
   */
  public final long getExpireTimestamp() {
    return this.expire_ts;
  }

  /**
   * Getter that returns the Unix epoch timestamp when the object was created
   * @return Unix epoch timestamp
   */
  public final long getCreated() {
    return created;
  }

  public final long getFileSize() {
    return this.file_size;
  }
}

/**
 * This is a small class containing simple metadata about cached objects such as
 * the size and how many times it's been hit. It's used for creating a sorted
 * list and figuring out what to eject
 */
class CacheEntryMeta implements Comparable<CacheEntryMeta> {
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
  public CacheEntryMeta(final int i, final int h, final int ds) {
    id = i;
    hits = h;
    data_size = ds;
  }

  /**
   * Comparator to determine what order the entries are sorted. It's based on
   * the hits and then the size of the data in cache with larger objects getting
   * preference TODO re-order this so smaller objects get preference since it
   * makes sense to hit the disk for something big, but not tiny
   */
  public int compareTo(CacheEntryMeta entry) {
    // compare hits first
    if (entry.hits < hits)
      return -1;
    else if (entry.hits > hits)
      return 1;

    // hits are the same so compare size next
    if (entry.data_size == data_size)
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
  public final int getID() {
    return id;
  }

  /**
   * Getter returning the data size
   * @return The data size
   */
  public final int getDataSize() {
    return data_size;
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage.hbase;

import java.nio.charset.Charset;
import java.util.ArrayList;

import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;

import org.hbase.async.ClientStats;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/** 
 * Shared client interface methods required to read and write to the HBase 
 * cluster
 */
final class HBaseStorageClient {
  private static final Logger LOG = 
    LoggerFactory.getLogger(HBaseStorageClient.class);
  
  /** How many time do we try to apply an edit before giving up. */
  private static short MAX_ATTEMPTS_PUT = 6;
  
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  private static short INITIAL_EXP_BACKOFF_DELAY = 800;
  
  /** Table name character set, we only need ASCII */
  private static Charset TABLE_CHARSET = Charset.forName("ISO-8859-1");
  
  /** The AsyncBase HBase client we'll use for connections */
  private final HBaseClient client;
  
  /** The table this HBase client is associated with */
  private final byte[] table;

  /** Configuration object */
  private final HBaseConfig config;
  
  /**
   * Constructor that initializes the HBase client, checks to make sure
   * the given table exists and sets up the config
   * @param config The parent OpenTSDB config object to load base settings from
   * @param table The table this client will interact with
   */
  public HBaseStorageClient(final Config config, final String table) {
    this.client = new HBaseClient(
        config.getString("tsd.storage.hbase.zk_quorum"),
        config.getString("tsd.storage.hbase.zk_basedir")
      );
    this.table = table.getBytes(TABLE_CHARSET);
    this.config = new HBaseConfig(config);
    
  }
  
  /**
   * Checks to see if the given table exists. This should be called immediately
   * after constructing the client before trying to execute any methods
   * @throws HBaseException if there was an issue
   * @throws TableNotFoundException if the table wasn't found
   * @throws Exception if something goes horribly wrong
   */
  public final void ensureTableExists() throws Exception {
    this.client.ensureTableExists(table).joinUninterruptibly();
  }
  
  /**
   * Gracefully shuts down the client connection
   * @return An object to wait on
   * @throws HBaseException if there was a problem
   */
  public final Deferred<Object> shutdown() {
    return client.shutdown();
  }
  
  /**
   * Flushes any queued RPCs to the cluster, including buffered datapoints
   * @return An object to wait
   * @throws HBaseException if there was a problem
   */
  public final Deferred<Object> flush() {
    return client.flush();
  }
  
  /**
   * Retrieves stats from the HBase client and stores them in the collector
   * @param collector The collector to store data in
   */
  public final void collectStats(final StatsCollector collector){
    final ClientStats stats = this.client.stats();
    if (stats == null) {
      return;
    }
    collector.record("hbase.connections.created", stats.connectionsCreated());
    collector.record("hbase.root_lookups", stats.rootLookups());
    collector.record("hbase.gets", stats.gets());
    collector.record("hbase.deletes", stats.deletes());
    collector.record("hbase.puts", stats.puts());
    collector.record("hbase.flushes", stats.flushes());
    collector.record("hbase.increments", stats.atomicIncrements());
    collector.record("hbase.meta_lookups.contended", stats.contendedMetaLookups());
    collector.record("hbase.nsr.exceptions", stats.noSuchRegionExceptions());
    collector.record("hbase.rpc.batched.sent", stats.numBatchedRpcSent());
    collector.record("hbase.rpc.delayed_nsre", stats.numRpcDelayedDueToNSRE());
    collector.record("hbase.locks.row", stats.rowLocks());
    collector.record("hbase.scanners.opened", stats.scannersOpened());
    collector.record("hbase.scans", stats.scans());    
  }
  
  /**
   * Attempts to retrieve the latest value for a specific cell
   * @param key The row key to fetch
   * @param family The name of the column family to fetch from
   * @param qualifier The cell qualifier to find
   * @return The raw binary data if the cell was found, null if not found or
   * the cell was empty
   * @throws Exception  if something goes pear shaped
   */
  public final byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier) throws Exception {
    return getValue(key, family, qualifier, null);
  }
  
  /**
   * Attempts to retrieve a specific cell value given a row, family and
   * qualifier with an optional lock held.
   * <b>Note:</b> This method is synchronous and will wait for the client to
   * respond before returning data.
   * @param key The row key to fetch
   * @param family The name of the column family to fetch from
   * @param qualifier The cell qualifier to find
   * @param lock An optional lock, should be null if not used
   * @return The raw binary data if the cell was found, null if not found or
   * the cell was empty
   * @throws Exception if something goes pear shaped
   */
  public final byte[] getValue(final byte[] key, 
      final byte[] family, final byte[] qualifier, final RowLock lock) 
    throws Exception {
    final GetRequest get = new GetRequest(table, key);
    if (lock != null) {
      get.withRowLock(lock);
    }
    get.family(family).qualifier(qualifier);
    final ArrayList<KeyValue> row = client.get(get).joinUninterruptibly();
    if (row == null || row.isEmpty()) {
      return null;
    }
    return row.get(0).value();
  }
      
  /**
   * Returns all of the values in a given row
   * 
   * Note: This can potentially return a ton of data so be careful
   * @param key The row key to fetch from
   * @return An array of KeyValue objects if successful, null if the row was
   * not found
   * @throws HBaseException if there's an issue fetching data
   */
  public final Deferred<ArrayList<KeyValue>> getRow(final byte[] key) {
    final GetRequest get = new GetRequest(table, key);
    return client.get(get);
  }
  
  /**
   * Attempts to delete one or more cells in a row and column family
   * @param key The row key to delete data from
   * @param family The column family
   * @param qualifiers An array of qualifier IDs to delete
   * @param lock An optional lock on the row, set to null if not using
   * @return An object to wait on if necessary
   */
  public final Deferred<Object> deleteValues(final byte[] key, final byte[] family,
      final byte[][] qualifiers, final RowLock lock) {
    final DeleteRequest dr = new DeleteRequest(table, key, family, qualifiers);
    return client.delete(dr);
  }
  
  /**
   * Stores the data without buffering and using the WAL using the system
   * time for the timestamp
   * @param key The row key
   * @param family The column family
   * @param qualifier The cell qualifier
   * @param data The data to store
   * @param lock An optional lock
   * @return An object to wait on 
   * @throws HBaseException if the put failed
   * @throws RuntimeException if the thread was interrupted
   * @throws IllegalStateException if something goes pear shaped
   */
  public final Deferred<Object> putDurableWithRetry(final byte[] key, 
      final byte[] family, final byte[] qualifier, final byte[] data, 
      final RowLock lock) {
    return putWithRetry(key, family, qualifier, data, 0, lock, true, false);
  }
  
  /**
   * Attempts to store the value in HBase with {@code MAX_ATTEMPTS_PUT} attempts
   * Each attempt will wait a bit longer before retrying and once all retries
   * are exhausted, the method will throw an exception
   * <b>Note:</b> If the timestamp is 0, the current system time will be used
   * @param key The row key
   * @param family The column family
   * @param qualifier The cell qualifier
   * @param data The data to store
   * @param ts A timestamp in milliseconds. If the value is 0, the current 
   * system time will be used
   * @param lock An optional lock
   * @param durable Whether or not to use the Write Ahead Log in HBase
   * @param bufferable Whether or not the put needs to be immediate or can be
   * queued for writing later on as a part of a batch
   * @return An object to wait on 
   * @throws HBaseException if the put failed
   * @throws RuntimeException if the thread was interrupted
   * @throws IllegalStateException if something goes pear shaped
   */
  public final Deferred<Object> putWithRetry(final byte[] key, final byte[] family, 
      final byte[] qualifier, final byte[] data, final long ts,
      final RowLock lock, final Boolean durable, final Boolean bufferable) {
    short attempts = MAX_ATTEMPTS_PUT;
    short wait = INITIAL_EXP_BACKOFF_DELAY;
    final PutRequest put;
    if (lock != null) {
      put = new PutRequest(this.table, key, family, qualifier, data, 
          ts < 1 ? System.currentTimeMillis() : ts, lock);
    } else {
      put = new PutRequest(this.table, key, family, qualifier, data, 
          ts < 1 ? System.currentTimeMillis() : ts);
    }
    put.setDurable(durable);
    put.setBufferable(bufferable);
    while (attempts-- > 0) {
      try {
        return client.put(put);
      } catch (HBaseException e) {
        if (attempts > 0) {
          LOG.warn("Put failed, attempts left=" + attempts + " (retrying in "
              + wait + " ms), put=" + put, e);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            throw new RuntimeException("interrupted", ie);
          }
          wait *= 2;
        } else {
          throw e;
        }
      }
    }
    throw new IllegalStateException("Failed a put attempt without an exception");
  }
  
  /**
   * Attempts to acquire a row lock on the given row
   * <b>Note:</b> This is a synchronous method that will wait until the lock
   * has been acquired. 
   * @param key The row to fetch a lock on
   * @return A lock object if acquired successfully
   * @throws HBaseException if the lock could not be acquired
   * @throws Exception If something goes horribly wrong
   */
  public final RowLock getRowLock(final byte[] key) throws Exception {
    return client.lockRow(new RowLockRequest(table, key))
    .joinUninterruptibly();
  }
  
  /**
   * Attempts to release the row lock gracefully
   * @param lock The lock to release
   * @throws UnknownRowLockException if the lock wasn't held or found, you can
   * generally ignore these
   * @throws HBaseException if there was an error releasing the lock
   * @throws Exception If something goes horribly wrong
   */
  public final void releaseRowLock(final RowLock lock) throws Exception {
    client.unlockRow(lock).joinUninterruptibly();
  }
  
  /**
   * Returns a new scanner for this client's table
   * @return A scanner object
   * @throws HBaseException if there was an error releasing the lock
   */
  public final Scanner newScanner() {
    return client.newScanner(this.table);
  }
  
  /** @return The name of the table this client is using */
  public final String table() {
    return new String(this.table, TABLE_CHARSET);
  }
}

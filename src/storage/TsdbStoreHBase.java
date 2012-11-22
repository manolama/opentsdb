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
package net.opentsdb.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.ConfigLoader;
import net.opentsdb.core.TsdbConfig;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;

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
import org.hbase.async.UnknownRowLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.stumbleupon.async.Deferred;

/**
 * The main interface for accessing HBase
 * 
 */
public class TsdbStoreHBase extends TsdbStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(TsdbStoreHBase.class);

  /** HBase client to use. */
  private HBaseClient client;
  private final HBaseConfig local_config;
  private final boolean use_zk_locks;
  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
  private CuratorFramework zk_client = null;

  /**
   * Initializes the TsdbStore class, sets the table and the HBase client object
   * @param table Default table to use for connections
   * @param client HBase async client
   */
  public TsdbStoreHBase(final TsdbConfig config, final byte[] table) {
    super(config, table);
    this.local_config = new HBaseConfig(config.getConfigLoader());
    this.use_zk_locks = local_config.useZKLocks();
    this.client = new HBaseClient(local_config.zookeeperQuorum(), 
        local_config.zookeeperBaseDirectory());
  }
  
  public void copy(final TsdbStore store){
    this.client = ((TsdbStoreHBase)store).client;
  }
  
  /**
   * Attempts to retrieve the latest value for a specific cell within a locked
   * key
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @param rowLock An object referring to a currently held row lock. Note that
   *          this method will not retrieve the lock for you, you have to call
   *          getLock() first.
   * @return A byte array with the contents of the cell if successful, a NULL if
   *         the cell wasn't found
   * @throws TsdbStorageException
   */
  public byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier, final Object rowLock) throws TsdbStorageException {
//    LOG.trace(String.format("Fetching key [%s]family [%s] qualifier [%s] lock [%s]", 
//        UniqueId.IDtoString(key), fromBytes(family), fromBytes(qualifier),
//        rowLock == null ? "no" : "yes"));
    final GetRequest get = new GetRequest(table, key);
    if (rowLock != null && !this.use_zk_locks)
      get.withRowLock((RowLock) rowLock);
    get.family(family).qualifier(qualifier);
    try {
      final ArrayList<KeyValue> row = client.get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        //LOG.trace("Nothing found in storage");
        return null;
      }
      //LOG.trace(String.format("Found [%s] cells w [%s]", row.size(), fromBytes(row.get(0).value())));
      return row.get(0).value();
    } catch (HBaseException e) {
      throw new TsdbStorageException(e.getMessage(), e);
    } catch (Exception e) {
      throw new TsdbStorageException("Unhandled exception ocurred", e);
    }
  }

  /**
   * Attempts to retrieve all of the cells for the given row, including all CFs
   * @param key The row to retrieve
   * @return An array of key/value pairs if successful, null if there wasn't any
   *         data
   * @throws TsdbStorageException
   */
  public Deferred<ArrayList<KeyValue>> getRow(final byte[] key)
      throws TsdbStorageException {
    final GetRequest get = new GetRequest(table, key);
    try {
      return client.get(get);
    } catch (HBaseException e) {
      throw new TsdbStorageException(e.getMessage(), e);
    } catch (Exception e) {
      throw new TsdbStorageException("Unhandled exception ocurred", e);
    }
  }

  /**
   * Attempts to delete the contents of a specific cell within a locked key
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifiers An array of cell names in the row to delete
   * @param rowLock An object referring to a currently held row lock. Note that
   *          this method will not retrieve the lock for you, you have to call
   *          getLock() first.
   * @return True if successful, false if the cell wasn't found
   * @throws TsdbStorageException
   */
  public Deferred<Object> deleteValues(final byte[] key, final byte[] family,
      final byte[][] qualifiers, final Object rowLock)
      throws TsdbStorageException {
    try {
      final DeleteRequest dr = new DeleteRequest(table, key, family, qualifiers);
      LOG.trace(dr.toString());
      //return Deferred.fromResult(null);
      return client.delete(dr);
    } catch (HBaseException e) {
      throw new TsdbStorageException(e.getMessage(), e);
    } catch (Exception e) {
      throw new TsdbStorageException(
          "Unexpected exception attempting to delete row", e);
    }
  }

  /**
   * Attempts to store a value in the storage system at the given location
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @param data The data to store
   * @param rowLock An object referring to a currently held row lock. Note that
   *          this method will not retrieve the lock for you, you have to call
   *          getLock() first.
   * @param durable Determines if the put is durable or not
   * @param bufferable Determines if the put is bufferable or not
   * @return True if the put was successful (or queued successfully if async)
   *         False if there was an error
   * @throws TsdbStorageException
   */
  public Deferred<Object> putWithRetry(final byte[] key,
      final byte[] family, final byte[] qualifier, final byte[] data, final long ts,
      final Object rowLock, final Boolean durable, final Boolean bufferable) 
      throws TsdbStorageException {
    // data check
    if (key == null){
      LOG.error("Missing key value");
      return null;
    }
    if (data == null){
      LOG.error("Missing data value");
      return null;
    }
    
    short attempts = MAX_ATTEMPTS_PUT;
    short wait = INITIAL_EXP_BACKOFF_DELAY;
    final PutRequest put;
    if (rowLock != null && !this.use_zk_locks)
      put = new PutRequest(this.table, key, family, qualifier, data, 
          ts < 1 ? System.currentTimeMillis() : ts, 
          (RowLock) rowLock);
    else
      put = new PutRequest(this.table, key, family, qualifier, data, 
          ts < 1 ? System.currentTimeMillis() : ts);
    put.setDurable(durable);
    put.setBufferable(bufferable);
    while (attempts-- > 0) {
      try {
        return client.put(put);
      } catch (HBaseException e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts + " (retrying in "
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
      } catch (Exception e) {
        LOG.error("WTF?  Unexpected exception type, put=" + put, e);
      }
    }
    throw new IllegalStateException("This code should never be reached!");
  }

  /**
   * Attempts to gain a lock on a row
   * @param key The row ID to lock
   * @return A lock object specific to the underlying structure, or NULL if one
   *         wasn't obtained in time
   * @throws TsdbStorageException
   */
  public Object getRowLock(final byte[] key) throws TsdbStorageException {
    try {
      if (this.use_zk_locks){
        if (this.zk_client == null){
          LOG.debug("Initializing Zookeeper lock client");
            this.zk_client = CuratorFrameworkFactory.newClient(local_config.zookeeperQuorum(), 
                retryPolicy);
            this.zk_client.start();
        }
        
        final String lock_path = "/locks/opentsdb/" + UniqueId.IDtoString(key);
        //LOG.trace(String.format("Attempting to acquire lock on [%s]", lock_path));
        
        InterProcessMutex lock = new InterProcessMutex(this.zk_client, lock_path);
        if ( lock.acquire(500, TimeUnit.MILLISECONDS) ) 
        {
          //LOG.trace(String.format("Successfully acquired lock on [%s]", lock_path));
          return lock;
        }

        LOG.warn(String.format("Unable to acquire ZK lock [%s] in 500ms", lock_path));
        lock = null;
        return null;
      }else{
        return client.lockRow(new RowLockRequest(table, key))
            .joinUninterruptibly();
      }
    } catch (HBaseException e) {
      LOG.warn("Failed to lock the [" + UniqueId.IDtoString(key) + "] row", e);
      throw new TsdbStorageException(e.getMessage(), e);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    } catch (Exception e) {
      throw new TsdbStorageException("Should never be here", e);
    }
  }

  /**
   * Attempts to release a row lock gained from getRowLock()
   * @param lock The lock to release
   * @return True if successful (or it was already unlocked), false if there was
   *         an error
   * @throws TsdbStorageException
   */
  public Boolean releaseRowLock(final Object lock) throws TsdbStorageException {
    if (lock == null)
      return true;
    try {
      try {
        if (this.use_zk_locks){
          InterProcessMutex zk_lock = (InterProcessMutex)lock;
          zk_lock.release();
          LOG.trace(String.format("Successfully released lock on [%s]", zk_lock.toString()));
          zk_lock = null;
          return true;
        }else{
          client.unlockRow((RowLock) lock).joinUninterruptibly();
        }
      } catch (UnknownRowLockException urle){
        LOG.warn("Lock was already released or invalid");
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return true;
    } catch (HBaseException e) {
      LOG.error("Error while releasing the lock on row ", e);
      throw new TsdbStorageException(e.getMessage(), e);
    }
  }

  /**
   * Attempts to open a scanner formatted for the underlying storage system
   * 
   * NOTE: you need to configure the incomming TsdbScanner with start and end
   * rows and optionally a column family, row regex and table. If the scanner
   * doesn't have a table set, it will use the client's table
   * 
   * @param scanner A TsdbScanner object to store state information in
   * @return A TsdbScanner object if successful, null if there was an error
   * @throws TsdbStorageException
   */
  public TsdbScanner openScanner(TsdbScanner scanner)
      throws TsdbStorageException {

    final Scanner scnr = this.client
        .newScanner(scanner.getTable() == null ? this.table : scanner
            .getTable());

    if (scanner.getStart_row() != null)
      scnr.setStartKey(scanner.getStart_row());
    if (scanner.getEndRow() != null)
      scnr.setStopKey(scanner.getEndRow());
    if (scanner.getFamily() != null)
      scnr.setFamily(scanner.getFamily());
    if (scanner.getRowRegex() != null && scanner.getRowRegex().length() > 0)
      scnr.setKeyRegexp(scanner.getRowRegex(), CHARSET);
    if (scanner.getQualifier() != null)
      scnr.setQualifier(scanner.getQualifier());
    scnr.setMaxNumKeyValues(scanner.getMaxKeyValues());
    scnr.setMaxNumRows(scanner.getMaxRows());

    scanner.setScanner(scnr);
    return scanner;
  }

  /**
   * Attempts to retrieve a set of rows from the scanner NOTE: you have to call
   * openScanner() first before trying to fetch any rows
   * @param scanner An open TsdbScanner object
   * @return An array of key/value pairs, or null if there isn't any more data
   *         or an error occurred.
   * @throws TsdbStorageException
   */
  public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(TsdbScanner scanner)
      throws TsdbStorageException {
    if (scanner == null)
      throw new TsdbStorageException("TsdbScanner object is null");
    if (scanner.getScanner() == null)
      throw new TsdbStorageException("HBase scanner object is null");

    try {
      Scanner scnr = (Scanner) scanner.getScanner();
      return scnr.nextRows();
    } catch (Exception e) {
      throw new TsdbStorageException("Unable to cast Scanner", e);
    }
  }

  public Deferred<Object> shutdown() {
    return client.shutdown();
  }

  public Deferred<Object> flush() throws TsdbStorageException {
    return client.flush();
  }

  public void collectStats(final StatsCollector collector){
    ClientStats stats = this.client.stats();
    if (stats == null)
      return;
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
  
  // GETTERS AND SETTERS ------------------------------------------------

  public final void setTable(String table){
    this.table = toBytes(table);
  }
  
  public final short getFlushInterval() throws TsdbStorageException {
    return client.getFlushInterval();
  }

  public final void setFlushInterval(final short flush_interval) throws TsdbStorageException {
    client.setFlushInterval(flush_interval);
  }

  private static class HBaseConfig{
    private static final Logger LOG = LoggerFactory.getLogger(TsdbConfig.class);

    private ConfigLoader config = new ConfigLoader();
    
    /** Defaults to the localhost for connecting to Zookeeper */
    private String zookeeper_quorum = "localhost";
    /** Path under which is the znode for the -ROOT- region */
    private String zookeeper_base_directory = "/hbase";
    /** Whether or not to use Zookeeper for locking */
    private boolean use_zk_locks = false;
    
    public HBaseConfig(final ConfigLoader config){
      this.config = config;
    }
    
    /**
     * A comma delimited list of zookeeper hosts to poll for access to the HBase
     * cluster
     * @return The hosts to work with
     */
    public final String zookeeperQuorum() {
      try {
        return this.config.getString("tsd.storage.hbase.zk.quorum");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.zookeeper_quorum;
    }

    /**
     * Path under which is the znode for the -ROOT- region
     * @return Path
     */
    public final String zookeeperBaseDirectory() {
      try {
        return this.config.getString("tsd.storage.hbase.zk.basedirectory");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.zookeeper_base_directory;
    }

    public final boolean useZKLocks() {
      try {
        return this.config.getBoolean("tsd.storage.hbase.zk.uselocks");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.use_zk_locks;
    }
  }
}

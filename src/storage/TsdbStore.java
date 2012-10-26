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

import java.nio.charset.Charset;
import java.util.ArrayList;

import net.opentsdb.core.TsdbConfig;
import net.opentsdb.stats.StatsCollector;

import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * The base class for interfacing with a storage system
 */
public abstract class TsdbStore {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbStore.class);

  /** Charset used to convert Strings to byte arrays and back. */
  protected static Charset CHARSET = Charset.forName("ISO-8859-1");
  /** How many time do we try to apply an edit before giving up. */
  protected static short MAX_ATTEMPTS_PUT = 6;
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  protected static short INITIAL_EXP_BACKOFF_DELAY = 800;
  /** Amount of time to wait, in ms, to gain a lock */
  protected static short LOCK_TIMEOUT = 800;
  /** Table to work with */
  protected byte[] table;

  /**
   * Default Constructor requires a table name When extending this class, be
   * sure to pass in a table name
   */
  public TsdbStore(final TsdbConfig config, final byte[] table) {
    this.table = table;
  }
  
  public TsdbStore(final byte[] table, final TsdbStore store) {
    this.table = table;
    this.copy(store);
  }

  abstract public void copy(final TsdbStore store);
  
  /**
   * Attempts to retrieve the latest value for a specific cell and convert it to
   * a string
   * @param key The row key
   * @param family The column family
   * @param qualifier The qualifier
   * @return A string with the contents of the cell or null if there was an
   *         error
   * @throws TsdbStorageException
   */
  public String getValue(final String key, final String family,
      final String qualifier) throws TsdbStorageException {
    final byte[] response = getValue(toBytes(key), toBytes(family),
        toBytes(qualifier));
    if (response == null)
      return null;
    return fromBytes(response);
  }

  /**
   * Attempts to retrieve the latest value for a specific cell
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @return A byte array with the contents of the cell if successful, a NULL if
   *         the cell wasn't found
   * @throws TsdbStorageException
   */
  public byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier) throws TsdbStorageException {
    return getValue(key, family, qualifier, null);
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
  abstract public byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier, final Object rowLock) throws TsdbStorageException;

  /**
   * Attempts to retrieve the value, at the specified time, for a specific cell
   * 
   * NOTE: May not be implemented by extending classes
   * 
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @param timestamp The time
   * @return A byte array with the contents of the cell if successful, a NULL if
   *         the cell wasn't found
   * @throws TsdbStorageException
   */
  public byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier, final long timestamp) throws TsdbStorageException {
    LOG.warn("Method not implemented");
    throw new TsdbStorageException("Method not implemented");
  }

  /**
   * Attempts to retrieve all of the cells for the given row, including all CFs
   * @param key The row to retrieve
   * @return An array of key/value pairs if successful, null if there wasn't any data
   * @throws TsdbStorageException
   */
  abstract public Deferred<ArrayList<KeyValue>> getRow(final byte[] key)
      throws TsdbStorageException;

  /**
   * Attempts to delete the contents of a specific cell
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @return True if successful, false if the cell wasn't found
   * @throws TsdbStorageException
   */
  public Deferred<Object> deleteValue(final byte[] key, final byte[] family,
      final byte[] qualifier) throws TsdbStorageException {
    return deleteValues(key, family, new byte[][] { qualifier }, null);
  }
  
  /**
   * Attempts to delete the contents of a specific cell within a locked key
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifiers An array of cell names in the row to delete
   * @return True if successful, false if the cell wasn't found
   * @throws TsdbStorageException
   */
  public Deferred<Object> deleteValues(final byte[] key, final byte[] family,
      final byte[][] qualifiers) throws TsdbStorageException {
    return deleteValues(key, family, qualifiers, null);
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
  abstract public Deferred<Object> deleteValues(final byte[] key, final byte[] family,
      final byte[][] qualifiers, final Object rowLock) throws TsdbStorageException;

  /**
   * Attempts to store a value in the storage system at the given location
   * 
   * Simply passes the data on to putWithRetry with a NULL for the row lock
   * 
   * @param key The row key in bytes
   * @param family The column family in bytes
   * @param qualifier The name of the cell in bytes
   * @param data The data to store
   * @return True if the put was successful (or queued successfully if async)
   *         False if there was an error
   * @throws TsdbStorageException
   */
  public Deferred<Object> putWithRetry(final byte[] key, final byte[] family,
      final byte[] qualifier, final byte[] data) throws TsdbStorageException {
    return putWithRetry(key, family, qualifier, data, null);
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
   * @return True if the put was successful (or queued successfully if async)
   *         False if there was an error
   * @throws TsdbStorageException
   */
  public Deferred<Object> putWithRetry(final byte[] key,
      final byte[] family, final byte[] qualifier, final byte[] data,
      final Object rowLock) 
      throws TsdbStorageException{
    return putWithRetry(key, family, qualifier, data, rowLock, true, false);
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
  abstract public Deferred<Object> putWithRetry(final byte[] key,
      final byte[] family, final byte[] qualifier, final byte[] data,
      final Object rowLock, final Boolean durable, final Boolean bufferable) 
      throws TsdbStorageException;

  /**
   * Attempts to gain a lock on a row
   * @param key The row ID to lock
   * @return A lock object specific to the underlying structure, or NULL if one
   *         wasn't obtained in time
   * @throws TsdbStorageException
   */
  abstract public Object getRowLock(final byte[] key)
      throws TsdbStorageException;

  /**
   * Attempts to release a row lock gained from getRowLock()
   * @param lock The lock to release
   * @return True if successful (or it was already unlocked), false if there was
   *         an error
   * @throws TsdbStorageException
   */
  abstract public Boolean releaseRowLock(final Object lock)
      throws TsdbStorageException;

  /**
   * Attempts to open a scanner formatted for the underlying storage system
   * @param scanner A TsdbScanner object to store state information in
   * @return A TsdbScanner object if successful, null if there was an error
   * @throws TsdbStorageException
   */
  public TsdbScanner openScanner(TsdbScanner scanner)
      throws TsdbStorageException {
    LOG.warn("Method not implemented");
    throw new TsdbStorageException("Method not implemented");
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
    LOG.warn("Method not implemented");
    throw new TsdbStorageException("Method not implemented");
  }

  /**
   * Shuts down the client connection
   * @return ?
   * @throws TsdbStorageException
   */
  abstract public Deferred<Object> shutdown() throws TsdbStorageException;

  /**
   * Writes any pending changes to the storage system
   * @return ?
   * @throws TsdbStorageException
   */
  abstract public Deferred<Object> flush() throws TsdbStorageException;

  /**
   * Allows the tsd to fetch statistics about the underlying storage system.
   * @param collector Collector where stats will be written
   */
  abstract public void collectStats(final StatsCollector collector);
  
  // STATIC METHODS ----------------------------------------------------------
  /**
   * Uses the configured character set to convert the given string to a byte
   * array
   * @param s String to convert to a byte array
   * @return Byte array formatted using the configured character set
   */
  public static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }

  /**
   * Uses the configured character set to convert the given byte array to a
   * string
   * @param b Byte array to convert to a string
   * @return String formatted from the byte array using the character set
   */
  public static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  // GETTERS AND SETTERS ------------------------------------------------------

  public final byte[] getTable() {
    return this.table;
  }

  abstract public void setTable(String table);
  
  public final String getTableString() {
    return fromBytes(this.table);
  }

  public short getFlushInterval() throws TsdbStorageException {
    LOG.error("Method not implemented");
    throw new TsdbStorageException("Method not implemented");
  }
  
  public void setFlushInterval(final short flush_interval) throws TsdbStorageException{
    LOG.error("Method not implemented");
    throw new TsdbStorageException("Method not implemented");
  }
  
  // STATIC GETTERS AND SETTERS -----------------------------------------------

  public static final Charset getCharset() {
    return CHARSET;
  }

  public static final void setCharset(final Charset charset) {
    CHARSET = charset;
  }

  public static final short getMaxAttemptsPut() {
    return MAX_ATTEMPTS_PUT;
  }

  public static final void setMaxAttemptsPut(final short max_attempts) {
    MAX_ATTEMPTS_PUT = max_attempts;
  }

  public static final short getInitialExpBackoffDelay() {
    return INITIAL_EXP_BACKOFF_DELAY;
  }

  public static final void setInitialExpBackoffDelay(final short backoff) {
    INITIAL_EXP_BACKOFF_DELAY = backoff;
  }
}

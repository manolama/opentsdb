package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * These are static helpers that make it easy to write or read data from HBase 
 * using the async library
 */
public class TSDStore {
  private static final Logger LOG = LoggerFactory.getLogger(TSDStore.class);

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** How many time do we try to apply an edit before giving up. */
  private static final short MAX_ATTEMPTS_PUT = 6;
  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
  /** Table to work with */
  private final byte[] table;
  
  /** HBase client to use.  */
  private final HBaseClient client;
  
  public TSDStore(HBaseClient client, byte[] table){
    this.client = client;
    this.table = table;
  }
  
  // uses CHARSET to encode a string
  public static byte[] toBytes(final String s) {
    return s.getBytes(CHARSET);
  }
  
  // uses the CHARSET to get a string from the bytes
  public static String fromBytes(final byte[] b) {
    return new String(b, CHARSET);
  }

  /** Returns the cell of the specified row, using family:kind. */
  public byte[] hbaseGet(final byte[] row, final byte[] family, final byte[] qualifier) 
  throws HBaseException {
    return hbaseGet(row, family, qualifier, null);
  }
  
  /** Returns the cell of the specified row key, using family:kind. */
  public byte[] hbaseGet(final byte[] key, final byte[] family, final byte[] qualifier,
                          final RowLock lock) throws HBaseException {
    final GetRequest get = new GetRequest(table, key);
    if (lock != null) {
      get.withRowLock(lock);
    }
    get.family(family).qualifier(qualifier);
    try {
      final ArrayList<KeyValue> row = client.get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return row.get(0).value();
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public void hbasePutWithRetry(final byte[] key, final byte[] family, final byte[] qualifier,
      final byte[] data, final RowLock lock)
    throws HBaseException {    
    short attempts = MAX_ATTEMPTS_PUT;
    short wait = INITIAL_EXP_BACKOFF_DELAY;
    final PutRequest put = new PutRequest(table, key, family, qualifier, data, lock);
    put.setBufferable(false);  // TODO(tsuna): Remove once this code is async.
    while (attempts-- > 0) {
      try {
        client.put(put).joinUninterruptibly();
        return;
      } catch (HBaseException e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts
                    + " (retrying in " + wait + " ms), put=" + put, e);
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
  
  public RowLock lock(final byte[] row)throws HBaseException {
    try {
      return client.lockRow(new RowLockRequest(table, row)).joinUninterruptibly();
    } catch (HBaseException e) {
      LOG.warn("Failed to lock the ____ row", e);
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public void unlock(final RowLock lock) {
    try {
      client.unlockRow(lock);
    } catch (HBaseException e) {
      LOG.error("Error while releasing the lock on row `MAXID_ROW'", e);
    }
  }
}

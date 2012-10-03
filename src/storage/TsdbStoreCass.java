package net.opentsdb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.stats.StatsCollector;

import org.apache.thrift.TException;
import org.apache.cassandra.thrift.*;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

public class TsdbStoreCass extends TsdbStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(TsdbStoreCass.class);
      
  private Cassandra.Client client;
  
  public TsdbStoreCass(final byte[] table, final Cassandra.Client client) {
    super(table);
    this.client = client;
    try {
      this.client.set_keyspace(fromBytes(table));
    } catch (InvalidRequestException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void copy(final TsdbStore store){
    this.client = ((TsdbStoreCass)store).client;
  }
  
  public byte[] getValue(final byte[] key, final byte[] family,
      final byte[] qualifier, final Object rowLock) throws TsdbStorageException {
    
    ByteBuffer row = ByteBuffer.allocate(key.length);
    row.put(key);
    row.position(0);
    
    ColumnPath gcp = new ColumnPath();
    gcp.setColumn_family(fromBytes(family));
    gcp.setColumn(qualifier);
    
    ColumnOrSuperColumn  sc = new ColumnOrSuperColumn();
    try {
      sc = client.get(row, gcp, ConsistencyLevel.ONE);
      return sc.column.getValue();
    } catch (InvalidRequestException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NotFoundException e) {
      // TODO Auto-generated catch block
      //e.printStackTrace();
    } catch (UnavailableException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TimedOutException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public Deferred<ArrayList<KeyValue>> getRow(final byte[] key)
    throws TsdbStorageException {
    throw new TsdbStorageException("Not implemented");
//    final GetRequest get = new GetRequest(table, key);
//    try {
//      return client.get(get);
//    } catch (HBaseException e) {
//      throw new TsdbStorageException(e.getMessage(), e);
//    } catch (Exception e) {
//      throw new TsdbStorageException("Unhandled exception ocurred", e);
//    }
  }
  
  public Deferred<Object> deleteValues(final byte[] key, final byte[] family,
      final byte[][] qualifiers, final Object rowLock)
      throws TsdbStorageException {
    throw new TsdbStorageException("Not implemented");
  }
  
  public Deferred<Object> putWithRetry(final byte[] key,
      final byte[] family, final byte[] qualifier, final byte[] data,
      final Object rowLock, final Boolean durable, final Boolean bufferable) 
      throws TsdbStorageException {
    // data check
    if (table == null){
      LOG.error("Missing table value");
      return null;
    }
    if (key == null){
      LOG.error("Missing key value");
      return null;
    }
    if (qualifier == null){
      LOG.error("Missing qualifier value");
      return null;
    }
    if (data == null){
      LOG.error("Missing data value");
      return null;
    }
    
    short attempts = MAX_ATTEMPTS_PUT;
    short wait = INITIAL_EXP_BACKOFF_DELAY;
    while (attempts-- > 0) {
      try {
        LOG.trace(String.format("table [%s] key [%s] family [%s] qualifier [%s]  val [%s]",
            fromBytes(table), fromBytes(key), fromBytes(family), fromBytes(qualifier), fromBytes(data)));
        ColumnParent cp = new ColumnParent();
        cp.setColumn_family(fromBytes(family));
        ByteBuffer row = ByteBuffer.allocate(key.length);
        row.put(key);
        row.position(0);
        
        Column c = new Column();
        c.setName(qualifier);
        c.setValue(data);
        client.set_keyspace(fromBytes(this.table));
        client.insert(row, cp, c, ConsistencyLevel.ONE);       
        return Deferred.fromResult(null);
      } catch (InvalidRequestException ire){
        ire.printStackTrace();
        throw new TsdbStorageException(ire.getMessage(), ire);
      } catch (Exception e) {
        if (attempts > 0) {
          LOG.error("Put failed, attempts left=" + attempts + " (retrying in "
              + wait + " ms), put=", e);
          try {
            Thread.sleep(wait);
          } catch (InterruptedException ie) {
            throw new RuntimeException("interrupted", ie);
          }
          wait *= 2;
        } else {
          throw new TsdbStorageException(e.getMessage(), e);
        }
      }
    }
    throw new IllegalStateException("This code should never be reached!");
  }
  
  public Object getRowLock(final byte[] key) throws TsdbStorageException {
    return new Boolean(true);
  }
  
  public Boolean releaseRowLock(final Object lock) throws TsdbStorageException {
    return true;
  }
  
  public TsdbScanner openScanner(TsdbScanner scanner)
    throws TsdbStorageException {

    SlicePredicate predicate = new SlicePredicate();
    SliceRange sr = new SliceRange();
    sr.setStart(new byte[] { 0 });
    sr.setFinish(new byte[] { 0 });
    sr.setCount(scanner.getMaxRows());
    predicate.setSlice_range(sr);

    scanner.setScanner(predicate);
    return scanner;
  }
  
  public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(TsdbScanner scanner)
    throws TsdbStorageException {
    if (scanner == null)
      throw new TsdbStorageException("TsdbScanner object is null");
    if (scanner.getScanner() == null)
      throw new TsdbStorageException("Cassandra scanner object is null");
    
    try {
      SlicePredicate predicate = (SlicePredicate) scanner.getScanner();
      
      ColumnParent cp = new ColumnParent();
      cp.setColumn_family(fromBytes(scanner.getFamily()));
      
      // this is the range of keys we want
      KeyRange range = new KeyRange();
      range.setStart_key(scanner.getStart_row());
      range.setEnd_key(scanner.getEndRow());
      
      ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>>();
      // something crappy is happening where the keyspace keeps getting switched
      client.set_keyspace(fromBytes(this.table));
      List<KeySlice> rs = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE);
      
      long num_rows = 0;
      for (KeySlice ks : rs){
        ArrayList<KeyValue> row = new ArrayList<KeyValue>();
        for (ColumnOrSuperColumn sc : ks.columns){
          row.add(new KeyValue(ks.getKey(), sc.super_column.getName(), 
              sc.column.getName(), sc.column.timestamp / 1000, sc.column.getValue()));
          num_rows++;
        }
        rows.add(row);
      }
      if (num_rows < 1)
        return Deferred.fromResult(null);
      return Deferred.fromResult(rows);    

    } catch (Exception e) {
      e.printStackTrace();
      throw new TsdbStorageException("Unable to cast Scanner", e);
    }
  }

  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> flush() {
    return Deferred.fromResult(null);
  }

  public void collectStats(final StatsCollector collector){
    LOG.warn("Not implemented");
  }
  
  @Override
  public void setTable(String table) {
    this.table = toBytes(table);
    try {
      client.set_keyspace(table);
      LOG.debug(String.format("Configured keyspace to [%s]", table));
    } catch (InvalidRequestException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

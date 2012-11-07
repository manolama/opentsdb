package net.opentsdb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.ConfigLoader;
import net.opentsdb.core.TsdbConfig;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.cassandra.thrift.*;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.UnknownRowLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.stumbleupon.async.Deferred;

public class TsdbStoreCass extends TsdbStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(TsdbStoreCass.class);
      
  private Cassandra.Client client;
  private final CassandraConfig local_config;
  private final boolean use_zk_locks;
  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
  private CuratorFramework zk_client = null;
  
  public TsdbStoreCass(final TsdbConfig config, final byte[] table) {
    super(config, table);
    this.local_config = new CassandraConfig(config.getConfigLoader());
    this.use_zk_locks = local_config.useZKLocks();
    try {     
      TTransport tr = new TFramedTransport(new TSocket(local_config.host(), local_config.port()));
      TProtocol proto = new TBinaryProtocol(tr);
      this.client = new Cassandra.Client(proto);
      tr.open();
      
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
      final byte[] family, final byte[] qualifier, final byte[] data, final long ts,
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
//        LOG.trace(String.format("table [%s] key [%s] family [%s] qualifier [%s]  val [%s]",
//            fromBytes(table), fromBytes(key), fromBytes(family), fromBytes(qualifier), fromBytes(data)));
        ColumnParent cp = new ColumnParent();
        cp.setColumn_family(fromBytes(family));
        ByteBuffer row = ByteBuffer.allocate(key.length);
        row.put(key);
        row.position(0);
        
        Column c = new Column();
        c.setName(qualifier);
        c.setValue(data);
        if (ts > 0)
          c.setTimestamp(ts);
        else
          c.setTimestamp(System.currentTimeMillis());
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
        return new Boolean(true);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    } catch (Exception e) {
      throw new TsdbStorageException("Should never be here", e);
    }
  }
  
  public Boolean releaseRowLock(final Object lock) throws TsdbStorageException {
    if (lock == null)
      return true;
    try {
      if (this.use_zk_locks){
        InterProcessMutex zk_lock = (InterProcessMutex)lock;
        zk_lock.release();
        //LOG.trace(String.format("Successfully released lock on [%s]", zk_lock.toString()));
        zk_lock = null;
        return true;
      }
    } catch (UnknownRowLockException urle){
      LOG.warn("Lock was already released or invalid");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return true;
  }
  
  public TsdbScanner openScanner(TsdbScanner scanner)
    throws TsdbStorageException {

    SlicePredicate predicate = new SlicePredicate();
    SliceRange sr = new SliceRange();    
    sr.setStart("".getBytes(this.CHARSET));
    sr.setFinish("".getBytes(this.CHARSET));
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
      if (scanner.getLastKey() != null)
        range.setStart_key(scanner.getLastKey());
      else
        range.setStart_key(scanner.getStart_row());
      range.setEnd_key(scanner.getEndRow());
      
//      range.setStart_key(new byte[] { 0 });
//      range.setEnd_key(new byte[] { 0 });
      
      ArrayList<ArrayList<KeyValue>> rows = new ArrayList<ArrayList<KeyValue>>();
      // something crappy is happening where the keyspace keeps getting switched
      //client.set_keyspace(fromBytes(this.table));
      List<KeySlice> rs = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE);
      
      long num_rows = 0;
      for (KeySlice ks : rs){
        // if the end key is equal to the last key, then we've iterated through everything
        if (scanner.getLastKey() != null && 
            Bytes.equals(scanner.getLastKey(), ks.getKey())){
          LOG.trace("Reached final key");
          return Deferred.fromResult(null);
        }
        
        scanner.setLastKey(ks.getKey());
        ArrayList<KeyValue> row = new ArrayList<KeyValue>();
        for (ColumnOrSuperColumn sc : ks.columns){
          row.add(new KeyValue(ks.getKey(), toBytes("t"),
              sc.column.getName(), sc.column.timestamp, sc.column.getValue()));
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

  private static class CassandraConfig{
    private static final Logger LOG = LoggerFactory.getLogger(TsdbConfig.class);

    private ConfigLoader config = new ConfigLoader();
    
    private String host = "localhost";
    private int port = 9160;
    /** Whether or not to use Zookeeper for locking */
    private boolean use_zk_locks = false;
    /** Defaults to the localhost for connecting to Zookeeper */
    private String zookeeper_quorum = "localhost";
    
    public CassandraConfig(final ConfigLoader config){
      this.config = config;
    }
    
    /**
     * A comma delimited list of zookeeper hosts to poll for access to the HBase
     * cluster
     * @return The hosts to work with
     */
    public final String host() {
      try {
        return this.config.getString("tsd.storage.cassandra.host");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.host;
    }

    /**
     * 
     * @return Path
     */
    public final int port() {
      try {
        return this.config.getInt("tsd.storage.cassandra.port");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.port;
    }

    public final boolean useZKLocks() {
      try {
        return this.config.getBoolean("tsd.storage.cassandra.zk.uselocks");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.use_zk_locks;
    }
    
    /**
     * A comma delimited list of zookeeper hosts to poll for access to the HBase
     * cluster
     * @return The hosts to work with
     */
    public final String zookeeperQuorum() {
      try {
        return this.config.getString("tsd.storage.cassandra.zk.quorum");
      } catch (NullPointerException npe) {
        // return the default below
      } catch (NumberFormatException nfe) {
        LOG.warn(nfe.getLocalizedMessage());
      }
      return this.zookeeper_quorum;
    }
  }

}

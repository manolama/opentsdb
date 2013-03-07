package net.opentsdb.storage;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbConfig;
import net.opentsdb.stats.StatsCollector;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

public class TsdbMemcache extends Thread  {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbMemcache.class);
  
  private static ConcurrentLinkedQueue<OperationFuture<Boolean>> queue = 
    new ConcurrentLinkedQueue<OperationFuture<Boolean>>();
  private static final AtomicLong set_success = new AtomicLong();
  private static final AtomicLong set_fail = new AtomicLong();
  private MemcachedClient client;
  private final String hosts;
  
  public TsdbMemcache(final TsdbConfig config){
    this.hosts = config.storageMemcacheHosts();
    if (this.hosts.isEmpty()){
      LOG.debug("No hosts set for the mecache client");
      return;
    }
    try {
      this.client = new MemcachedClient(
          AddrUtil.getAddresses(this.hosts));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      this.client = null;
    }
  }
  
  public void run(){
    OperationFuture<Boolean> result;
    int count = 0;
    while(true){
      try{
        result = queue.poll();
        count = 0;
        if (result != null){
          count++;
          while (!result.isDone() && count < 100){
            Thread.sleep(10);
            count++;
          }            
          if (!result.isDone()){
            set_fail.incrementAndGet();
            LOG.debug("Memcache failure");
          }else
            
            set_success.incrementAndGet();
          continue;
        }
        
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }catch (Exception e){
        e.printStackTrace();
      }
    }
  }
  
  public static void collectStats(final StatsCollector collector){
    collector.record("memcache.set.success", set_success.get());
    collector.record("memcache.set.fail", set_fail.get());
  }
  
  public void AsyncSetKey(final String key, final String value, final int expiration){
    if (this.client != null){
      queue.add(this.client.set(key, expiration, value));
    }
  }
}

package net.opentsdb.storage;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TsdbConfig;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

public class TsdbMemcache {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbMemcache.class);
  
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
  
  public void AsyncSetKey(final String key, final String value, final int expiration){
    if (this.client != null){
      this.client.set(key, expiration, value);
    }
  }
}

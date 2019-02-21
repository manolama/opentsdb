package net.opentsdb.pools;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

public class StormPotPoolFactory extends BaseTSDBPlugin implements ObjectPoolFactory {
  
  @Override
  public ObjectPool newPool(final ObjectPoolConfig config) {
    StormPotPool pool = new StormPotPool(tsdb, config);
    return pool;
  }

  @Override
  public String type() {
    return "StormPotPoolFactory";
  }
  
  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    this.id = id;
    this.tsdb = tsdb;
    registerConfigs(tsdb.getConfig());
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }
  
  String myConfig(final String key) {
    return PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".") + key;
  }
  
  void registerConfigs(final Configuration config) {
    if (!config.hasProperty(myConfig(INITIAL_COUNT_KEY))) {
      config.register(myConfig(INITIAL_COUNT_KEY), 4096, false, "TODO");
    }
  }
  
}

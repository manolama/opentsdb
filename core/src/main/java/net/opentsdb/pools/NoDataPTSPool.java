package net.opentsdb.pools;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;

public class NoDataPTSPool extends BaseObjectPoolAllocator {

  public static final String TYPE = "NoDataPTSPool";
  private static final TypeToken<?> TYPE_TOKEN = 
      TypeToken.of(NoDataPartialTimeSeries.class);

  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    if (Strings.isNullOrEmpty(id)) {
      this.id = TYPE;
    } else {
      this.id = id;
    }
    
    registerConfigs(tsdb.getConfig(), TYPE);
    final ObjectPoolConfig config = DefaultObjectPoolConfig.newBuilder()
        .setAllocator(this)
        .setInitialCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
        .setMaxCount(tsdb.getConfig().getInt(configKey(COUNT_KEY, TYPE)))
        .setId(this.id)
        .build();
    try {
      createAndRegisterPool(tsdb, config, TYPE);
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
  }
  
  @Override
  public Object allocate() {
    return new NoDataPartialTimeSeries();
  }
  
  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }
  
}

package net.opentsdb.query.processor.downsample;

import com.google.common.base.Strings;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.pools.BaseObjectPoolAllocator;
import net.opentsdb.pools.DefaultObjectPoolConfig;
import net.opentsdb.pools.ObjectPoolConfig;

public class DownsamplePartialTimeSeriesSetPool extends BaseObjectPoolAllocator {
  public static final String TYPE = "DownsampleNumericPartialTimeSeries";
  public static final TypeToken<?> TYPE_TOKEN = TypeToken.of(
      DownsampleNumericPartialTimeSeries.class);

  private TSDB tsdb;
  
  @Override
  public String type() {
    return TYPE;
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
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
    return new DownsampleNumericPartialTimeSeries(tsdb);
  }
  
  @Override
  public TypeToken<?> dataType() {
    return TYPE_TOKEN;
  }

}

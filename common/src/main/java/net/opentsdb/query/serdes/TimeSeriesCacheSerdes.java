package net.opentsdb.query.serdes;

import java.util.Collection;
import java.util.Map;

import net.opentsdb.query.QueryResult;
import net.opentsdb.query.cache.QueryCachePlugin.CachedQueryResult;

public interface TimeSeriesCacheSerdes {

  public byte[] serialize(final Collection<QueryResult> results);

  public byte[][] serialize(final int[] timestamps, 
                            final Collection<QueryResult> results);
  
  public Map<String, CachedQueryResult> deserialize(final byte[] data);
  
}

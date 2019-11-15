package net.opentsdb.query.anomaly;

import com.stumbleupon.async.Deferred;

import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

public interface PredictionCache {
  
  public Deferred<QueryResult> fetch(final QueryPipelineContext context, 
                                     final byte[] key,  
                                     final Span upstream_span);
  
  public Deferred<Void> cache(final byte[] key,
                              final long expiration,
                              final QueryResult results,
                              final Span upstream_span);
  
  public Deferred<Void> delete(final byte[] key);
  
}

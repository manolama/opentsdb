package net.opentsdb.query;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryPipelineContext.ResultWrapper;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;
import net.opentsdb.query.cache.QueryCachePlugin;
import net.opentsdb.query.cache.QueryCachePlugin.CacheCB;
import net.opentsdb.query.cache.QueryCachePlugin.CacheQueryResults;
import net.opentsdb.query.cache.QueryCachePlugin.CachedQueryResult;
import net.opentsdb.query.execution.cache.CombinedArray;
import net.opentsdb.query.execution.cache.CombinedNumeric;
import net.opentsdb.query.execution.cache.CombinedResult;
import net.opentsdb.query.execution.cache.DefaultTimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.cache.TimeSeriesCacheKeyGenerator;
import net.opentsdb.query.execution.serdes.DummyQueryNode;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;

public class ReadCacheQueryPipelineContext extends AbstractQueryPipelineContext 
    implements CacheCB {
  static final Logger LOG = LoggerFactory.getLogger(
      ReadCacheQueryPipelineContext.class);
  
  public static final String CACHE_PLUGIN_KEY = "tsd.query.cache.plugin_id";
  public static final String KEYGEN_PLUGIN_KEY = "tsd.query.cache.keygen.plugin_id";
  
  protected final long current_time;
  protected int[] slices;
  protected int interval_in_seconds;
  protected String string_interval;
  protected int min_interval;
  protected boolean skip_cache;
  protected QueryCachePlugin cache;
  protected TimeSeriesCacheKeyGenerator key_gen;
  protected boolean tip_query;
  protected byte[][] keys;
  protected long[] expirations;
  protected ResultOrSubQuery[] results;
  protected AtomicInteger latch;
  protected AtomicInteger hits;
  protected AtomicBoolean failed;
  protected QueryContext sub_context;
  protected List<QueryResult> sub_results;
  
  ReadCacheQueryPipelineContext(final QueryContext context, 
                                final List<QuerySink> direct_sinks) {
    super(context);
    if (direct_sinks != null && !direct_sinks.isEmpty()) {
      sinks.addAll(direct_sinks);
    }
    failed = new AtomicBoolean();
    current_time = DateTime.currentTimeMillis();
    System.out.println("      CACHE CTX ID: " + System.identityHashCode(context));
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    registerConfigs(context.tsdb());
    
    cache = context.tsdb().getRegistry().getPlugin(QueryCachePlugin.class, 
        context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY));
    if (cache == null) {
      throw new IllegalArgumentException("No cache plugin found for: " +
          (Strings.isNullOrEmpty(
              context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY)) ? 
                  "Default" : context.tsdb().getConfig().getString(CACHE_PLUGIN_KEY)));
    }
    key_gen = context.tsdb().getRegistry()
        .getPlugin(TimeSeriesCacheKeyGenerator.class, 
            context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY));
    if (key_gen == null) {
      throw new IllegalArgumentException("No key gen plugin found for: " + 
          (Strings.isNullOrEmpty(
              context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY)) ? 
                  "Default" : context.tsdb().getConfig().getString(KEYGEN_PLUGIN_KEY)));
    }
    
    // TODO - pull this out into another shared function.
    // For now we find the highest common denominator for intervals.
    
    // TODO - issue: If we have a downsample of 1w, we can't query on 1 day segments
    // so we either cache the whole shebang or we bypass the cache.
    interval_in_seconds = 0;
    int ds_interval = Integer.MAX_VALUE;
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      final QueryNodeFactory factory = context.tsdb().getRegistry()
          .getQueryNodeFactory(DownsampleFactory.TYPE);
      if (factory == null) {
        LOG.error("WTF!!!!!!!!!!!!!! NULL DS????");
      }
      if (((DownsampleFactory) factory).intervals() == null) {
        LOG.error("WTF????????????? NULLI NTERVALS");
      }
      if (config instanceof DownsampleConfig) {
        String interval;
        if (((DownsampleConfig) config).getRunAll()) {
          skip_cache = true;
          break;
        } else if (((DownsampleConfig) config).getOriginalInterval()
            .toLowerCase().equals("auto")) {
          final long delta = context.query().endTime().msEpoch() - 
              context.query().startTime().msEpoch();
          interval = DownsampleFactory.getAutoInterval(delta, 
              ((DownsampleFactory) factory).intervals());
          System.out.println("       AUTO: " + interval);
        } else {
          // normal interval
          interval = ((DownsampleConfig) config).getInterval();
        }
        
        int parsed = (int) DateTime.parseDuration(interval) / 1000;
        if (parsed < ds_interval) {
          ds_interval = parsed;
        }
        System.out.println("          ds_interval: " + ds_interval + "   IN: " + interval);
        if (parsed > interval_in_seconds) {
          System.out.println("final interval: " + interval + "  Parsed: " + parsed);
          interval_in_seconds = parsed;
          string_interval = interval;
        }
      }
    }
    
    if (skip_cache) {
      // don't bother doing anything else.
      return Deferred.fromResult(null);
    }
    
    // TODO - in the future use rollup config. For now snap to one day.
    // AND 
    if (interval_in_seconds >= 3600) {
      interval_in_seconds = 86400;
      string_interval = "1d";
    } else {
      interval_in_seconds = 3600;
      string_interval = "1h";
    }
    
    if (ds_interval == Integer.MAX_VALUE) {
      min_interval = 0;
    } else {
      min_interval = ds_interval;
    }
    
    // TODO - validate calendaring. May need to snap differently based on timezone.
    long start = context.query().startTime().epoch();
    start = start - (start % interval_in_seconds);
    
    long end = context.query().endTime().epoch();
    end = end - (end % interval_in_seconds);
    if (end != context.query().endTime().epoch()) {
      end += interval_in_seconds;
    }
    
    slices = new int[(int) ((end - start) / interval_in_seconds)];
    int ts = (int) start;
    for (int i = 0; i < slices.length; i++) {
      slices[i] = ts;
      ts += interval_in_seconds;
    }
    expirations = new long[slices.length];
    expirations[0] = min_interval;
    keys = key_gen.generate(context.query().buildHashCode().asLong(), string_interval, slices, expirations);
    if (tip_query) {
      keys = Arrays.copyOf(keys, keys.length - 1);
    }
    results = new ResultOrSubQuery[slices.length];
    
    if (context.sinkConfigs() != null) {
      for (final QuerySinkConfig config : context.sinkConfigs()) {
        final QuerySinkFactory factory = context.tsdb().getRegistry()
            .getPlugin(QuerySinkFactory.class, config.getId());
        if (factory == null) {
          throw new IllegalArgumentException("No sink factory found for: " 
              + config.getId());
        }
        
        final QuerySink sink = factory.newSink(context, config);
        if (sink == null) {
          throw new IllegalArgumentException("Factory returned a null sink for: " 
              + config.getId());
        }
        sinks.add(sink);
        if (sinks.size() > 1) {
          throw new UnsupportedOperationException("Only one sink allowed for now, sorry!");
        }
      }
    }
    
    System.out.println("CACHE SINK CONFIGS......... " + context.sinkConfigs() 
      + "  AND SINKS: " + sinks);
    return Deferred.fromResult(null);
  }

  @Override
  public void fetchNext(final Span span) {
    hits = new AtomicInteger();
    latch = new AtomicInteger(slices.length);
    cache.fetch(this, keys, this, null);
  }
  
  @Override
  public void onCacheResult(final CacheQueryResults result) {
    if (failed.get()) {
      return;
    }
    
    try {
      ResultOrSubQuery ros = null;
      int idx = 0;
      for (int i = 0; i < keys.length; i++) {
        if (Bytes.memcmp(keys[i], result.key()) == 0) {
          synchronized (results) {
            results[i] = new ResultOrSubQuery();
            results[i].key = result.key();
            if (result.results() != null && !result.results().isEmpty()) {
              results[i].map = Maps.newHashMapWithExpectedSize(result.results().size());
              for (final Entry<String, CachedQueryResult> entry : result.results().entrySet()) {
                results[i].map.put(entry.getKey(), entry.getValue());
              }
            }
          }
          ros = results[i];
          idx = i;
          break;
        }
      }
      
      if (ros == null) {
        onCacheError(-1, new RuntimeException("Whoops, got a result that wasn't in "
            + "our keys? " + Arrays.toString(result.key())));
        return;
      }
      
      if (ros.map == null || ros.map.isEmpty()) {
        System.out.println("  UH OH No hit!");
        // TODO - configure the threshold
        if (okToRunMisses(hits.get())) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Running sub query for interval at: " + slices[idx]);
          }
          if (query().isTraceEnabled()) {
            context.logTrace("Running sub query for interval at: " + slices[idx]);
          }
          ros.sub_context = ros.sub_context = buildQuery(slices[idx], slices[idx] + interval_in_seconds, context, ros);
          ros.sub_context.initialize(null)
            .addCallback(new SubQueryCB(ros.sub_context))
            .addErrback(new ErrorCB());
        }
      } else {
        if (okToRunMisses(hits.incrementAndGet())) {
          runCacheMissesAfterSatisfyingPercent();
        }
        if (requestTip(idx, result.lastValueTimestamp())) {
          System.out.println("     TIP QUERY but not at final latch");
          ros.map = null;
          if (okToRunMisses(hits.get())) {
            latch.incrementAndGet();
            if (LOG.isTraceEnabled()) {
              LOG.trace("Running sub query for interval at: " + slices[idx]);
            }
            if (query().isTraceEnabled()) {
              context.logTrace("Running sub query for interval at: " + slices[idx]);
            }
            ros.sub_context = ros.sub_context = buildQuery(slices[idx], slices[idx] + interval_in_seconds, context, ros);
            ros.sub_context.initialize(null)
              .addCallback(new SubQueryCB(ros.sub_context))
              .addErrback(new ErrorCB());
          }
        } else {
          ros.complete.set(true);
        }
      }
      
      if (latch.decrementAndGet() == 0) {
        // all cache are in, see if we should send up or if we need to fire
        // sub queries.
        processResults();
      }
    } catch (Throwable t) {
      onCacheError(-1, t);
    }
  }

  @Override
  public void onCacheError(final int index, final Throwable t) {
    if (failed.compareAndSet(false, true)) {
      LOG.warn("Failure from cache", t);
      onError(t);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Failure from cache after initial failure", t);
    }
  }
  
  @Override
  public void close() {
    cleanup();
    try {
      super.close();
    } catch (Throwable t) {
      LOG.warn("failed to close super", t);
    }
  }
  
  void processResults() {
    System.out.println("----------- PROCESSING");
    if (hits.get() < keys.length) {
      for (int i = 0; i < results.length; i++) {
        if (!results[i].complete.get()) {
          System.out.println("        not done??: " + i);
          if (okToRunMisses(hits.get())) {
            runCacheMissesAfterSatisfyingPercent();
          } else {
            // We failed the cache threshold so we run a FULL query.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Too many cache misses: " + hits.get() + " out of " + slices.length + "; running the full query.");
            }
            if (query().isTraceEnabled()) {
              context.logTrace("Too many cache misses: " + hits.get() + " out of " + slices.length + "; running the full query.");
            }
            if (sub_context != null) {
              System.out.println("WTF???? SUB CONTEXT != null?");
              throw new IllegalStateException("WTF???? SUB CONTEXT != null?");
            }
            sub_context = buildQuery(
                slices[0], 
                slices[slices.length - 1] + interval_in_seconds, 
                context, 
                new FullQuerySink());
            System.out.println("  FULL QUERY ID: " + System.identityHashCode(sub_context));
            sub_context.initialize(null)
                .addCallback(new SubQueryCB(sub_context))
                .addErrback(new ErrorCB());
            
            // while that's running, release the old resources
            cleanup();
          }
          return;
        }
      }
    }
    
    try {
      // sort and merge
      Map<String, QueryResult[]> sorted = Maps.newHashMap();
      for (int i = 0; i < results.length; i++) {
        if (results[i] == null) {
          System.out.println("null at................... " + i);
          continue;
        }
        for (final Entry<String, QueryResult> entry : results[i].map.entrySet()) {
          QueryResult[] qrs = sorted.get(entry.getKey());
          if (qrs == null) {
            qrs = new QueryResult[results.length + (tip_query ? 1 : 0)];
            sorted.put(entry.getKey(), qrs);
          }
          qrs[i] = entry.getValue();
        }
      }
      
      latch.set(sorted.size());
      for (final Entry<String, QueryResult[]> results : sorted.entrySet()) {
        // TODO - implement
        // TODO - send in thread pool
        DummyQueryNode n = new DummyQueryNode(results.getKey());
        for (final QuerySink sink : sinks) {
          sink.onNext(new CombinedResult(this, results.getValue(), n, results.getKey(), sinks, latch, string_interval));
        }
      }
      
      for (int i = 0; i < results.length; i++) {
        final int x = i;
        if (results[i].sub_context == null) {
          continue;
        }
        
        // write to the cache
        if (context.query().getCacheMode() == CacheMode.NORMAL ||
            context.query().getCacheMode() == CacheMode.WRITEONLY) {
          context.tsdb().getQueryThreadPool().submit(new Runnable() {
            @Override
            public void run() {
              cache.cache(slices[x], keys[x], expirations[x], results[x].map.values());
            }
          }, context);
        }
      }
    
    } catch (Throwable t) {
      LOG.error("Failed to process results", t);
      onError(t);
    }
  }
  
  /** @return if we found a query we couldn't cache. */
  public boolean skipCache() {
    return skip_cache;
  }
  
  class ResultOrSubQuery implements QuerySink {
    byte[] key;
    QueryContext sub_context;
    volatile Map<String, QueryResult> map = Maps.newConcurrentMap();
    AtomicBoolean complete = new AtomicBoolean();
    
    @Override
    public void onComplete() {
      if (failed.get()) {
        return;
      }
      
      complete.compareAndSet(false, true);
      if (latch.decrementAndGet() == 0) {
        System.out.println("[[[[[[[[ COMPLETE!!! ]]]]]]] RUNNING");
        processResults();
      }
    }
    
    @Override
    public void onNext(final QueryResult next) {
      if (failed.get()) {
        return;
      }
      
      final String id = next.source().config().getId() + ":" + next.dataSource();
      if (map == null) {
        synchronized (this) {
          if (map == null) {
            map = Maps.newConcurrentMap();
          }
        }
      }
      map.put(id, next);
      if (next instanceof ResultWrapper) {
        ((ResultWrapper) next).closeWrapperOnly();
      }
      System.out.println(" SET IT!!!!!!!!!");
    }
    
    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
    }
    
    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        ReadCacheQueryPipelineContext.this.onError(t);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in sub query after initial failure", t);
        }
      }
    }
    
  }
  
  static QueryContext buildQuery(final int start, 
                                 final int end, 
                                 final QueryContext context, 
                                 final QuerySink sink) {
    final SemanticQuery.Builder builder = ((SemanticQuery) context.query())
        .toBuilder()
        .setCacheMode(CacheMode.BYPASS)
        .setStart(Integer.toString(start))
        .setEnd(Integer.toString(end));
    
    return SemanticQueryContext.newBuilder()
        .setTSDB(context.tsdb())
        .setLocalSinks((List<QuerySink>) Lists.newArrayList(sink))
        .setQuery(builder.build())
        .setStats(context.stats())
        .setAuthState(context.authState())
        .build();
  }

  void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(CACHE_PLUGIN_KEY)) {
      tsdb.getConfig().register(CACHE_PLUGIN_KEY, null, true, 
          "The ID of a cache plugin to use.");
    }
    if (!tsdb.getConfig().hasProperty(KEYGEN_PLUGIN_KEY)) {
      tsdb.getConfig().register(KEYGEN_PLUGIN_KEY, null, true,
          "The ID of a key generator plugin to use.");
    }
  }
  
  void runCacheMissesAfterSatisfyingPercent() {
    synchronized (results) {
      for (int i = 0; i < results.length; i++) {
        final ResultOrSubQuery ros = results[i];
        if (ros == null) {
          continue;
        }
        
        if (ros.sub_context == null && 
            (ros.map == null || ros.map.isEmpty())) {
          latch.incrementAndGet();
          ros.sub_context = buildQuery(slices[i], slices[i] + 
              interval_in_seconds, context, ros);
          ros.sub_context.initialize(null)
            .addCallback(new SubQueryCB(ros.sub_context))
            .addErrback(new ErrorCB());
        }
      }
    }
  }
  
  boolean okToRunMisses(final int hits) {
    return hits > 0 && ((double) hits / (double) keys.length) > .60;
  }
  
  boolean requestTip(final int index, final TimeStamp ts) {
    // if the index is earlier than the final two buckets then we know we
    // don't need to request any data as it's old enough.
    if (index < results.length - 3 || ts == null) {
      System.out.println(" IDX : " + index);
      return false;
    }
    
    System.out.println("DELTA: " + (current_time - ts.msEpoch()) + "  MIN: " + (min_interval * 1000));
    if (current_time - ts.msEpoch() > (min_interval * 1000)) {
      return false;
    }
    return true;
  }
  
  void cleanup() {
    for (int i = 0; i < results.length; i++) {
      if (results[i].map != null) {
        for (final QueryResult result : results[i].map.values()) {
          try {
            result.close();
          } catch (Throwable t) {
            LOG.warn("Failed to close result", t);
          }
        }
      }
      
      results[i].map = null;
      if (results[i].sub_context != null) {
        try {
          results[i].sub_context.close();
        } catch (Throwable t) {
          LOG.warn("Failed to close sub context", t);
        }
      }
    }
  }
  
  class SubQueryCB implements Callback<Void, Void> {
    final QueryContext context;
    
    SubQueryCB(final QueryContext context) {
      this.context = context;
    }
    
    @Override
    public Void call(final Void arg) throws Exception {
      context.fetchNext(null);
      return null;
    }
    
  }
  
  class ErrorCB implements Callback<Void, Exception> {

    @Override
    public Void call(final Exception e) throws Exception {
      if (failed.compareAndSet(false, true)) {
        onError(e);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in sub query after initial failure", e);
        }
      }
      return null;
    }
    
  }

  class FullQuerySink implements QuerySink {

    @Override
    public void onComplete() {
      if (latch.decrementAndGet() == 0) {
        for (final QuerySink sink : sinks) {
          sink.onComplete();
        }
      }
      
      // write to the cache
      if (context.query().getCacheMode() == CacheMode.NORMAL ||
          context.query().getCacheMode() == CacheMode.WRITEONLY) {
        context.tsdb().getQueryThreadPool().submit(new Runnable() {
          @Override
          public void run() {
            try {
              cache.cache(slices, keys, expirations, sub_results);
            } catch (Throwable t) {
              LOG.error("Failed to cache the data", t);
            } finally {
//              for (int i = 0; i < sub_results.size(); i++) {
//                sub_results.get(i).close();
//              }
            }
          }
        }, context);
      } else {
//        for (int i = 0; i < sub_results.size(); i++) {
//          sub_results.get(i).close();
//        }
      }
    }
    
    @Override
    public void onNext(final QueryResult next) {
      latch.incrementAndGet();
      synchronized (ReadCacheQueryPipelineContext.this) {
        if (sub_results == null) {
          sub_results = Lists.newArrayList();
        }
        sub_results.add(next);
      }
      // TODO - do we need to wrap this?
      //ReadCacheQueryPipelineContext.this.onNext(next);
      for (final QuerySink sink : sinks) {
        try {
          sink.onNext(next);
        } catch (Throwable e) {
          LOG.error("Exception thrown passing results to sink: " + sink, e);
          // TODO - should we kill the query here?
        }
      }
//      if (next instanceof ResultWrapper) {
//        ((ResultWrapper) next).closeWrapperOnly();
//      }
    }
    
    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
    }
    
    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        ReadCacheQueryPipelineContext.this.onError(t);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in main query after initial failure", t);
        }
      }
    }
    
  }
}

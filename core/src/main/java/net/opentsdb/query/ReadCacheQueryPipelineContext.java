package net.opentsdb.query;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.hash.HashCode;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryPipelineContext.ResultWrapper;
import net.opentsdb.query.QueryNodeConfig.Builder;
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
import net.opentsdb.query.execution.serdes.JsonV2QuerySerdesOptions;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.query.processor.merge.MergerConfig;
import net.opentsdb.query.processor.summarizer.Summarizer;
import net.opentsdb.query.processor.summarizer.SummarizerConfig;
import net.opentsdb.query.processor.summarizer.SummarizerFactory;
import net.opentsdb.query.serdes.SerdesOptions;
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
  protected QueryContext full_query_context;
  protected List<QueryResult> sub_results;
  //protected Collection<QueryNode> summarizer_sink;
  protected Map<String, QueryNode> summarizer_node_map;
  
  ReadCacheQueryPipelineContext(final QueryContext context, 
                                final List<QuerySink> direct_sinks) {
    super(context);
    if (direct_sinks != null && !direct_sinks.isEmpty()) {
      sinks.addAll(direct_sinks);
    }
    failed = new AtomicBoolean();
    current_time = DateTime.currentTimeMillis();
    //summarizer_sink = Lists.newArrayList((QueryNode) this);
    System.out.println("      CACHE CTX ID: " + System.identityHashCode(context));
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    registerConfigs(context.tsdb());
    System.out.println("[[[[[[[[ S: " + context.query().startTime().epoch() + " E: " + context.query().endTime().epoch() + " DELTA: " + 
    ((context.query().endTime().epoch() - context.query().startTime().epoch()) / 60));
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
        
        if (parsed > interval_in_seconds) {
          interval_in_seconds = parsed;
          string_interval = interval;
        }
      }
    }
    System.out.println(" ###### INTERVAL IN SECONDS:  " + interval_in_seconds);
    
    if (skip_cache) {
      // don't bother doing anything else.
      return Deferred.fromResult(null);
    }

    class CB implements Callback<Void, ArrayList<Void>> {
      final int ds_interval;
      
      CB(final int ds_interval) {
        this.ds_interval = ds_interval;
      }
      
      @Override
      public Void call(ArrayList<Void> arg) throws Exception {
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
        System.out.println("[[[[[[[[ AFTER summarizer S: " + context.query().startTime().epoch() + " E: " + context.query().endTime().epoch() + " DELTA: " + 
            ((context.query().endTime().epoch() - context.query().startTime().epoch()) / 60));
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
        System.out.println("  TIMESTAMPS: " + Arrays.toString(slices));
        expirations = new long[slices.length];
        expirations[0] = min_interval * 1000; // needs to be in millis
        keys = key_gen.generate(context.query().buildHashCode().asLong(), 
            string_interval, slices, expirations);
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
        
        final Set<String> serdes_sources = computeSerializationSources();
        for (final String source : serdes_sources) {
          countdowns.put(source, new AtomicInteger(sinks.size()));
        }
        for (Entry<String, AtomicInteger> entry : countdowns.entrySet()) {
          System.out.println(" COUNTDOWNS: " + entry.getKey() + ", " + entry.getValue().get());
        }
        
        System.out.println("CACHE SINK CONFIGS......... " + context.sinkConfigs() 
          + "  AND SINKS: " + sinks);
        return null;
      }
      
    }
    
    // strip summarizers as we need to join the underlying results first and then
    // sum over all of them. If we run the full query, we need to tweak the query
    // to get the data feeding into the summarizer and cache *that*, not the
    // summary.
    
    // TODO - handle the case wherein a summary is in the middle of a DAG. That
    // could happen. For now we assume it's always at the end.
    Map<String, QueryNodeConfig> summarizers = Maps.newHashMap();
    List<QueryNodeConfig> new_execution_graph = Lists.newArrayList();
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      if (config instanceof SummarizerConfig) {
        summarizers.put(config.getId(), config);
      } else {
        new_execution_graph.add(config);
      }
    }
    
    if (summarizers.isEmpty()) {
      // no-op
    } else {
      SemanticQuery.Builder builder = ((SemanticQuery) context.query()).toBuilder();
      Set<String> new_serdes_filter = Sets.newHashSet();
      if (context.query().getSerdesConfigs() != null && 
          !context.query().getSerdesConfigs().isEmpty()) {
        // TODO - genercize for all types of filters
        // TODO - handle multiple filters
        // add summarizer sources
        for (final QueryNodeConfig config : summarizers.values()) {
          // wtf? figure this out
          for (final Object source : config.getSources()) {
            new_serdes_filter.add((String) source);
          }
        }
        
        for (final SerdesOptions config : context.query().getSerdesConfigs()) {
          for (final String id : config.getFilter()) {
            if (!summarizers.containsKey(id)) {
              new_serdes_filter.add(id);
            }
          }
        }
        
        builder.setExecutionGraph(new_execution_graph);
        builder.setSerdesConfigs(Lists.newArrayList(
            JsonV2QuerySerdesOptions.newBuilder()
                .setFilter(Lists.newArrayList(new_serdes_filter))
                .setId("serdes")
                .build()
            ));
        System.out.println(" ****** RESET QUERY and stripped summarizers.");
        ((BaseQueryContext) context).resetQuery(builder.build());
      }
      
      // now compute the DAG
      summarizer_node_map = Maps.newHashMap();
      QueryNodeFactory factory = context.tsdb().getRegistry().getQueryNodeFactory(
          SummarizerFactory.TYPE);
      if (factory == null) {
        throw new IllegalStateException("No factory for summary??");
      }
      ArrayList<Deferred<Void>> deferreds = Lists.newArrayList();
      for (QueryNodeConfig config : summarizers.values()) {
        // pass through or not?
        boolean pass_through = false;
        for (final Object source : config.getSources()) {
          if (new_serdes_filter.contains((String) source)) {
            pass_through = true;
            break;
          }
        }
        
        if (pass_through) {
          config = ((SummarizerConfig) config).toBuilder()
              .setPassThrough(true)
              .setSources(config.getSources())
              .build();
        }
        
        final QueryNode summarizer = factory.newNode(this, config);
        for (final Object source : config.getSources()) {
          summarizer_node_map.put((String) source, summarizer);
        }
        deferreds.add(summarizer.initialize(span));
      }
      return Deferred.group(deferreds).addCallback(new CB(ds_interval));
    }
    
    try {
      return Deferred.fromResult(new CB(ds_interval).call(null));
    } catch (Exception e) {
      return Deferred.fromError(e);
    }
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
    // TODO - race here I think
    if (hits.get() < keys.length) {
      for (int i = 0; i < results.length; i++) {
        if (!results[i].complete.get()) {
          System.out.println("        not done??: " + i);
          if (okToRunMisses(hits.get())) {
            runCacheMissesAfterSatisfyingPercent();
          } else {
            // We failed the cache threshold so we run a FULL query.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Too many cache misses: " + (slices.length - hits.get()) + " out of " + slices.length + "; running the full query.");
            }
            if (query().isTraceEnabled()) {
              context.logTrace("Too many cache misses: " +  (slices.length - hits.get()) + " out of " + slices.length + "; running the full query.");
            }
            if (full_query_context != null) {
              System.out.println("WTF???? SUB CONTEXT != null?");
              throw new IllegalStateException("WTF???? SUB CONTEXT != null?");
            }
            full_query_context = buildQuery(
                slices[0], 
                slices[slices.length - 1] + interval_in_seconds, 
                context, 
                new FullQuerySink());
            System.out.println("  FULL QUERY ID: " + System.identityHashCode(full_query_context));
            full_query_context.initialize(null)
                .addCallback(new SubQueryCB(full_query_context))
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
        final QueryResult result = new CombinedResult(this, results.getValue(), n, results.getKey(), sinks, latch, string_interval);
        QueryNode summarizer = summarizer_node_map != null ? summarizer_node_map.get(n.config().getId()) : null; 
        if (summarizer != null) {
          summarizer.onNext(result);
        } else {
          onNext(result);
//        for (final QuerySink sink : sinks) {
//          sink.onNext(new CombinedResult(this, results.getValue(), n, results.getKey(), sinks, latch, string_interval));
//        }
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
  
  @Override
  public Collection<QueryNode> upstream(final QueryNode node) {
    return Lists.newArrayList(this);
//    if (node instanceof Summarizer) {
//      return Collection.
//    }
//    final QueryNode summarizer = summarizer_node_map.get(node.config().getId());
//    if (summarizer == null) {
//      throw new IllegalArgumentException("No summarizer found for " + node.config().getId());
//    }
//    return Lists.newArrayList(summarizer);
  }
  
  @Override
  public Collection<QueryNode> downstream(final QueryNode node) {
    return Collections.emptyList();
  }
  
  @Override
  public Collection<TimeSeriesDataSource> downstreamSources(final QueryNode node) {
    return Collections.emptyList();
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
      
      // don't cache summaries so we avoid reading them out.
      if (!(next.source() instanceof Summarizer)) {
        final String id = next.source().config().getId() + ":" + next.dataSource();
        if (map == null) {
          synchronized (this) {
            if (map == null) {
              map = Maps.newConcurrentMap();
            }
          }
        }
        map.put(id, next);
      }
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
//      if (latch.decrementAndGet() == 0) {
//        for (final QuerySink sink : sinks) {
//          sink.onComplete();
//        }
//      }
      
      // write to the cache
//      if (context.query().getCacheMode() == CacheMode.NORMAL ||
//          context.query().getCacheMode() == CacheMode.WRITEONLY) {
//        context.tsdb().getQueryThreadPool().submit(new Runnable() {
//          @Override
//          public void run() {
//            try {
//              cache.cache(slices, keys, expirations, sub_results);
//            } catch (Throwable t) {
//              LOG.error("Failed to cache the data", t);
//            } finally {
////              for (int i = 0; i < sub_results.size(); i++) {
////                sub_results.get(i).close();
////              }
//            }
//          }
//        }, context);
//      } else {
////        for (int i = 0; i < sub_results.size(); i++) {
////          sub_results.get(i).close();
////        }
//      }
    }
    
    @Override
    public void onNext(final QueryResult next) {
      if (next.source() instanceof Summarizer) {
        System.out.println("        ROUTING to AQPC: " + next.source().config().getId());
        ReadCacheQueryPipelineContext.this.onNext(next);
        return;
      }
      
      //latch.incrementAndGet();
      synchronized (ReadCacheQueryPipelineContext.this) {
        if (sub_results == null) {
          sub_results = Lists.newArrayList();
        }
        sub_results.add(next);
      }
      
      final QueryNode summarizer = summarizer_node_map.get(next.source().config().getId());
      if (summarizer != null) {
        System.out.println("        ROUTING to summarizer: " + next.source().config().getId());
        summarizer.onNext(next);
      } else {
        System.out.println("        ROUTING to AQPC: " + next.source().config().getId());
        ReadCacheQueryPipelineContext.this.onNext(next);
      }
      // TODO - do we need to wrap this?
      //ReadCacheQueryPipelineContext.this.onNext(next);
//      for (final QuerySink sink : sinks) {
//        try {
//          sink.onNext(next);
//        } catch (Throwable e) {
//          LOG.error("Exception thrown passing results to sink: " + sink, e);
//          // TODO - should we kill the query here?
//        }
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
  
  @Override
  protected boolean checkComplete() {
    if (super.checkComplete()) {
      if (context.query().getCacheMode() == CacheMode.NORMAL ||
          context.query().getCacheMode() == CacheMode.WRITEONLY) {
        context.tsdb().getQueryThreadPool().submit(new Runnable() {
          @Override
          public void run() {
            try {
              System.out.println("CACHING: " + sub_results.get(0).source().config().getId());
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
    for (Entry<String, AtomicInteger> entry : countdowns.entrySet()) {
      System.out.println(" COUNTDOWNS: " + entry.getKey() + ", " + entry.getValue().get());
    }
    System.out.println("     NOT complete");
    return false;
//    for (final AtomicInteger integer : countdowns.values()) {
//      if (integer.get() > 0) {
//        return false;
//      }
//    }
//    
//    // done!
//    for (final QuerySink sink : sinks) {
//      try {
//        sink.onComplete();
//      } catch (Throwable t) {
//        LOG.error("Failed to close sink: " + sink, t);
//      }
//    }
//    return true;
  }
  
  /**
   * TODO - look at this to find a better way than having a generic
   * config.
   */
  class ContextNodeConfig implements QueryNodeConfig {

    @Override
    public String getId() {
      return "QueryContext";
    }
    
    @Override
    public String getType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> getSources() {
      // TODO Auto-generated method stub
      return null;
    }
    
    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return Const.HASH_FUNCTION().newHasher()
          .putInt(System.identityHashCode(this)) // TEMP!
          .hash();
    }

    @Override
    public boolean pushDown() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean joins() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Map<String, String> getOverrides() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getString(Configuration config, String key) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getInt(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getLong(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean getBoolean(Configuration config, String key) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public double getDouble(Configuration config, String key) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean hasKey(String key) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Builder toBuilder() {
      return null;
    }

    @Override
    public int compareTo(Object o) {
      return 0;
    }
  }

  Set<String> computeSerializationSources() {
    MutableGraph<QueryNodeConfig> config_graph = GraphBuilder.directed()
        .allowsSelfLoops(false)
        .build();
    
    Map<String, QueryNodeConfig> configs = Maps.newHashMap();
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      config_graph.addNode(config);
      configs.put(config.getId(), config);
    }
    for (final QueryNode summarizer : summarizer_node_map.values()) {
      configs.put(summarizer.config().getId(), summarizer.config());
      for (final Object source : summarizer.config().getSources()) {
        config_graph.putEdge(summarizer.config(), configs.get((String) source));
      }
    }
    // next pass
    for (final QueryNodeConfig config : context.query().getExecutionGraph()) {
      for (final Object source : config.getSources()) {
        config_graph.putEdge(config, configs.get((String) source));
      }
    }
    
    // find roots
    final Set<QueryNodeConfig> roots = Sets.newHashSet();
    for (final QueryNodeConfig config : config_graph.nodes()) {
      if (config_graph.predecessors(config).isEmpty()) {
        roots.add(config);
      }
    }
    
    Set<String> serdes_sources = Sets.newHashSet();
    for (final QueryNodeConfig root : roots) {
      System.out.println(" WORKING FROM ROOT: " + root.getId());
      serdes_sources.addAll(computeSerializationSources(config_graph, root));
    }
    System.out.println(" SERDES SOURCES: " + serdes_sources);
    return serdes_sources;
  }
  
  private Set<String> computeSerializationSources(
      final MutableGraph<QueryNodeConfig> config_graph, 
      final QueryNodeConfig node) {
    if (node instanceof TimeSeriesDataSourceConfig) {
      return Sets.newHashSet(((TimeSeriesDataSourceConfig) node).getDataSourceId());
    } else if (node.joins()) {
      return Sets.newHashSet(node.getId());
    }
    
    final Set<String> ids = Sets.newHashSetWithExpectedSize(1);
    for (final QueryNodeConfig downstream : config_graph.successors(node)) {
      final Set<String> downstream_ids = computeSerializationSources(config_graph, downstream);
      if (config_graph.predecessors(node).isEmpty()) {
        System.out.println("      COMPUTE ROOT: " + node.getId());
        // prepend
        if (downstream instanceof TimeSeriesDataSourceConfig) {
          ids.add(downstream.getId() + ":" 
              + ((TimeSeriesDataSourceConfig) downstream).getDataSourceId());
        } else if (downstream.joins()) {
          ids.addAll(downstream_ids);
        } else if (node instanceof SummarizerConfig &&
            ((SummarizerConfig) node).passThrough()) {
          System.out.println("                PASS THROUGH");
          for (final QueryNodeConfig successor : config_graph.successors(node)) {
            final Set<String> summarizer_sources = 
                computeSerializationSources(config_graph, successor);
            for (final String id : summarizer_sources) {
              ids.add(successor.getId() + ":" + id);
              ids.add(node.getId() + ":" + id);
            }
          }
        } else {
          for (final String id : downstream_ids) {
            ids.add(downstream.getId() + ":" + id);
          }
        }
      } else if (node instanceof MergerConfig) {
        ids.add(((MergerConfig) node).getDataSource());
      } else {
        ids.addAll(downstream_ids);
      }
    }
    return ids;
  }
}

package net.opentsdb.query.egads.olympicscoring;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.yahoo.egads.models.tsmm.OlympicModel2;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.alert.AlertTypeList;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseQueryContext;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.AbstractQueryPipelineContext.ResultWrapper;
import net.opentsdb.query.anomaly.PredictionCache;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.egads.EgadsResult;
import net.opentsdb.query.egads.EgadsThresholdTimeSeries;
import net.opentsdb.query.egads.EgadsTimeSeries;
import net.opentsdb.query.egads.ThresholdEvaluator;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.downsample.DownsampleFactory;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

/**
 * NOTE: Prediction and cached prediction maintains original metric IDs.
 *
 */
public class OlympicScoringNode extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      OlympicScoringNode.class);
  
  private final OlympicScoringConfig config;
  private final PredictionCache cache;
  private final byte[] cache_key;
  private final CountDownLatch latch;
  private final int jitter;
  private TemporalAmount jitter_duration;
  protected final AtomicBoolean failed;
  private BaselineQuery[] baseline_queries;
  private final TemporalAmount baseline_period;
  Properties properties;
  long prediction_start;
  private EgadsResult prediction;
  private QueryResult current;
  final TLongObjectMap<OlympicScoringBaseline> join = 
      new TLongObjectHashMap<OlympicScoringBaseline>();
  final ChronoUnit model_units;
  long prediction_intervals;
  long prediction_interval;
  
  public OlympicScoringNode(final QueryNodeFactory factory,
                            final QueryPipelineContext context,
                            final OlympicScoringConfig config) {
    super(factory, context);
    this.config = config;
    latch = new CountDownLatch(2);
    failed = new AtomicBoolean();
    baseline_period = DateTime.parseDuration2(config.getBaselinePeriod());
    
    // TODO - find the proper ds in graph in order
    DownsampleConfig ds = null;
    for (final QueryNodeConfig node : config.getBaselineQuery().getExecutionGraph()) {
      if (node instanceof DownsampleConfig) {
        ds = (DownsampleConfig) node;
        break;
      }
    }
    if (ds == null) {
      throw new IllegalStateException("Downsample can't be null.");
    }
    
    String ds_interval;
    final long query_time_span = context.query().endTime().msEpoch() - 
        context.query().startTime().msEpoch();
    if (ds.getInterval().equalsIgnoreCase("AUTO")) {
      final QueryNodeFactory dsf = context.tsdb().getRegistry()
          .getQueryNodeFactory(DownsampleFactory.TYPE);
      if (dsf == null) {
        LOG.error("Unable to find a factory for the downsampler.");
      }
      if (((DownsampleFactory) dsf).intervals() == null) {
        LOG.error("No auto intervals for the downsampler.");
      }
      ds_interval = DownsampleFactory.getAutoInterval(query_time_span, 
          ((DownsampleFactory) factory).intervals(), null);
    } else {
      ds_interval = ds.getInterval();
    }
    
    // set timings
    switch (config.getMode()) {
    case CONFIG:
      jitter = 0;
      cache_key = null;
      cache = null;
      model_units = null;
      prediction_start = context.query().startTime().epoch();
      prediction_interval = DateTime.parseDuration(ds_interval) / 1000;
      prediction_intervals = query_time_span / (prediction_interval * 1000);
      break;
    case EVALUATE:
    case PREDICT:
      long baseline_span = DateTime.parseDuration(config.getBaselinePeriod()) / 1000;
      if (baseline_span < 86400) {
        model_units = ChronoUnit.HOURS;
      } else {
        model_units = ChronoUnit.DAYS;
      }
      
      jitter = jitter();
      jitter_duration = Duration.ofSeconds(jitter);
      cache_key = null;
      cache = null; // PULL FROM FACTORY
      
      final TimeStamp start = context.query().startTime().getCopy();
      final ChronoUnit duration = modelDuration();
      start.snapToPreviousInterval(1, duration);
      start.add(jitter_duration);
      prediction_start = start.epoch();
      prediction_interval = model_units == ChronoUnit.HOURS ? 
          3600 : 86400;
      prediction_intervals = query_time_span / (prediction_interval * 1000);
      break;
    default:
      throw new IllegalStateException("Unhandled config mode: " + config.getMode());
    }
    
    System.out.println("  PRED START: " + prediction_start); // good is 11 now w/o jitter
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    final class InitCB implements Callback<Void, Void> {
      @Override
      public Void call(final Void arg) throws Exception {
        // trigger the cache lookup.
        if (cache != null) {
          cache.fetch(pipelineContext(), generateCacheKey(), null)
            .addCallback(new CacheCB())
            .addErrback(new CacheErrCB());
        } else {
          fetchBaselineData();
        }
        return null;
      }
    }
    return super.initialize(span).addCallback(new InitCB());
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(final QueryResult next) {
    LOG.info("GOT CURRENT: " + next.dataSource());
    if (next.timeSpecification() != null) {
      LOG.info("CUR START: " + next.timeSpecification().start().epoch());
    }
    System.out.println("    GOT CURRENT: " + next.dataSource());
    current = next;
    countdown();
  }
  
  void run() {
    // Got baseline and current data, yay!
    final TLongObjectMap<TimeSeries> map = new TLongObjectHashMap<TimeSeries>();
    
    for (final TimeSeries series : current.timeSeries()) {
      final long hash = series.id().buildHashCode();
      System.out.println("  CUR: " + hash);
      map.put(hash, series);
    }
    LOG.info("SER THRESHOLDS: " + config.getSerializeThresholds());
    int series_limit = prediction.timeSeries().size();
    for (int i = 0; i < series_limit; i++) {
      final TimeSeries series = prediction.timeSeries().get(i);
      final long hash = ((EgadsTimeSeries) series).originalHash();
      TimeSeries cur = map.remove(hash);
      if (cur != null) {
        //runPair(cur, series);
        final ThresholdEvaluator eval = new ThresholdEvaluator(
            config.getUpperThreshold(),
            config.isUpperIsScalar(),
            config.getLowerThreshold(),
            config.isLowerIsScalar(),
            config.getSerializeThresholds() ? /* TODO */ 4096: 0,
            cur,
            current,
            series,
            prediction);
        eval.evaluate();
        if (eval.alerts() != null && !eval.alerts().isEmpty()) {
          ((EgadsTimeSeries) series).addAlerts(eval.alerts());
        }
        
        if (config.getSerializeThresholds()) {
          if (config.getUpperThreshold() != 0) {
            prediction.timeSeries().add(new EgadsThresholdTimeSeries(
                cur.id(), 
                "upper", 
                prediction.timeSpecification().start(), 
                eval.upperThresholds(), 
                eval.index(),
                OlympicScoringFactory.TYPE));
          }
          if (config.getLowerThreshold() != 0) {
            prediction.timeSeries().add(new EgadsThresholdTimeSeries(
                cur.id(), 
                "lower", 
                prediction.timeSpecification().start(), 
                eval.lowerThresholds(), 
                eval.index(),
                OlympicScoringFactory.TYPE));
          }
        }
      }
    }
    
    // TODO - iterate through the final things in the map and push them out without
    // predictions.
    if (config.getSerializeObserved()) {
      // yeah, ew, but it's an EgadsResult so we have an array list.
      prediction.timeSeries().addAll(current.timeSeries());
    }
    sendUpstream(prediction);
  }
  
  void runBaseline() {
    TypeToken<? extends TimeSeriesId> id_type = null;
    properties = new Properties();
     properties.setProperty("TS_MODEL", "OlympicModel2"); // Model to be used
    //       properties.setProperty("INTERVAL", Integer.toString(
    //           DateTime.getDurationInterval(config.getTsdWindowSize())));
     properties.setProperty("INTERVAL", "1");
    //       properties.setProperty("INTERVAL_UNITS", 
    //           DateTime.getDurationChronoUnits(config.getTsdWindowSize()).toString());
     properties.setProperty("INTERVAL_UNITS", "MINUTES");
     // TODO - pull from config
    //       properties.setProperty("WINDOW_SIZE", Integer.toString( 
    //           DateTime.getDurationInterval(modelSpan)));
     if (config.getMode() == ExecutionMode.CONFIG) {
       properties.setProperty("WINDOW_SIZE", Long.toString(
           context.query().endTime().epoch() - context.query().startTime().epoch()));
       properties.setProperty("WINDOW_SIZE_UNITS", "SECONDS");
     } else {
       properties.setProperty("WINDOW_SIZE", "1");
       properties.setProperty("WINDOW_SIZE_UNITS", model_units.toString());
     }
     properties.setProperty("WINDOW_DISTANCE", Integer.toString(
         DateTime.getDurationInterval(config.getBaselinePeriod())));
     properties.setProperty("WINDOW_DISTANCE_UNITS", 
         DateTime.unitsToChronoUnit(
         DateTime.getDurationUnits(config.getBaselinePeriod())).toString());
     properties.setProperty("HISTORICAL_WINDOWS", Integer.toString(
         config.getBaselineNumPeriods()));
     properties.setProperty("WINDOW_AGGREGATOR", 
         config.getBaselineAggregator().toUpperCase());
     properties.setProperty("MODEL_START", Long.toString(prediction_start));
     properties.setProperty("ENABLE_WEIGHTING", "TRUE");
     properties.setProperty("AGGREGATOR",
         config.getBaselineAggregator().toUpperCase());
     properties.setProperty("NUM_TO_DROP_LOWEST", 
         Integer.toString(config.getExcludeMin()));
     properties.setProperty("NUM_TO_DROP_HIGHEST", 
         Integer.toString(config.getExcludeMax()));
     properties.setProperty("PERIOD", 
         Long.toString(prediction_interval * prediction_intervals));
    
     // TODO - parallelize
    List<TimeSeries> computed = Lists.newArrayList();
    TLongObjectIterator<OlympicScoringBaseline> it = join.iterator();
    while (it.hasNext()) {
      it.advance();
      TimeSeries ts = it.value().predict(properties);
      if (ts != null) {
        computed.add(ts);
      } else {
        System.out.println(" ------- NULL SERIES from the predictor!!!");
      }
    }
    TimeStamp start = new SecondTimeStamp(prediction_start);
    TimeStamp end = start.getCopy();
    end.add(modelDuration() == ChronoUnit.HOURS ? Duration.ofHours(1) : Duration.ofDays(1));

    if (cache != null) {
      // need's a clone as we may modify the list when we add thresholds, etc.
      writeCache(new EgadsResult(this, start, end, Lists.newArrayList(computed), id_type));
    }
    prediction = new EgadsResult(this, start, end, computed, id_type);
    countdown();
  }
  
  class CacheCB implements Callback<Void, QueryResult> {

    @Override
    public Void call(final QueryResult result) throws Exception {
      // TODO
      // If result == null ? fire baseline, else store and use to match.
      if (result != null) {
        // TODO - wrap into the OSResult
        //prediction = result;
        countdown();
      } else {
        fetchBaselineData();
      }
      
      return null;
    }
    
  }
  
  class CacheErrCB implements Callback<Void, Exception> {

    @Override
    public Void call(final Exception e) throws Exception {
      LOG.warn("Cache exception", e);
      fetchBaselineData();
      return null;
    }
    
  }
  
  class BaselineQuery implements QuerySink {
    final int idx;
    QueryContext sub_context;
    
    BaselineQuery(final int idx) {
      this.idx = idx;
    }
    
    @Override
    public void onComplete() {
      System.out.println(" SUB QUERY " + idx + " COMPLETE!!!");
      if (failed.get()) {
        return;
      }
      
//      if (sub_context != null && sub_context.logs() != null) {
//        ((BaseQueryContext) context).appendLogs(sub_context.logs());
//      }
      
//      complete.compareAndSet(false, true);  
//      if (baselineDataIn()) {
//        runBaseline();
//      }
      

      if (idx + 1 < baseline_queries.length) {
        // fire next
        baseline_queries[idx + 1].sub_context.initialize(null)
          .addCallback(new SubQueryCB(baseline_queries[idx + 1].sub_context))
          .addErrback(new ErrorCB());
      } else {
        runBaseline();
      }
    }

    @Override
    public void onNext(final QueryResult next) {
      System.out.println(" SUB QUERY " + idx + " NEXT " + next.dataSource());
      // TODO filter, for now assume one result
      
      for (final TimeSeries series : next.timeSeries()) {
        final long hash = series.id().buildHashCode();
        System.out.println("[" + idx + "]   RAW BASELINE HASH: " + hash);
        OlympicScoringBaseline baseline = join.get(hash);
        if (baseline == null) {
          baseline = new OlympicScoringBaseline(OlympicScoringNode.this, series.id());
          join.put(hash, baseline);
          System.out.println("      NEW Baseline: " + hash);
        } else {
          System.out.println("      Existing baseline: " + hash);
        }
        baseline.append(series, next);
      }
      
      next.close();
    }

    @Override
    public void onNext(final PartialTimeSeries next, 
                       final QuerySinkCallback callback) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onError(final Throwable t) {
      if (failed.compareAndSet(false, true)) {
        LOG.error("OOOPS on sub query: " + idx + " " + t.getMessage());
        OlympicScoringNode.this.onError(t);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failure in baseline query after initial failure", t);
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
      System.out.println("      FETCH NEXT ON CONTEXT: " + context);
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
        LOG.warn("Failure launching baseline query after initial failure", e);
      }
      return null;
    }
    
  }

  void fetchBaselineData() {
    System.out.println("------- FETCHING BASELINES");
    baseline_queries = new BaselineQuery[config.getBaselineNumPeriods()];
    final TimeStamp start = new SecondTimeStamp(prediction_start);
//    final ChronoUnit duration = modelDuration();
//    start.snapToPreviousInterval(1, duration);
////    if (jitter > 0) {
////      start.add(jitter_duration);
////    }
    final TemporalAmount period = DateTime.parseDuration2(config.getBaselinePeriod());
    // advance to the oldest time first
    for (int i = 0; i < config.getBaselineNumPeriods(); i++) {
      start.subtract(period);
    }
    final TimeStamp end;
    if (config.getMode() == ExecutionMode.CONFIG) {
      end = context.query().endTime().getCopy();
    } else {
      end = start.getCopy();
      end.add(Duration.of(1, model_units));
    }
    //end.add(duration == ChronoUnit.DAYS ? Duration.ofDays(1) : Duration.ofHours(1));
//    baseline_start = start.epoch();
//    System.out.println("      BASELINE EPOCH: " + baseline_start);
    
    // fire!
    for (int i = 0; i < config.getBaselineNumPeriods(); i++) {
      final BaselineQuery query = new BaselineQuery(i);
      baseline_queries[i] = query;
      query.sub_context = buildQuery((int) start.epoch(), 
                                     (int) end.epoch(), 
                                     context.queryContext(), 
                                     query);
      start.add(baseline_period);
      end.add(baseline_period);
//      query.sub_context.initialize(null)
//        .addCallback(new SubQueryCB(query.sub_context))
//        .addErrback(new ErrorCB());
    }

    baseline_queries[0].sub_context.initialize(null)
      .addCallback(new SubQueryCB(baseline_queries[0].sub_context))
      .addErrback(new ErrorCB());
  }
  
  byte[] generateCacheKey() {
    // TODO - include: jitter timestamp, full query hash, model ID
    return null;
  }
  
  void countdown() {
    latch.countDown();
    System.out.println("    COUNTDOWN: " + latch.getCount());
    if (latch.getCount() == 0) {
      run();
    }
  }
  
  QueryContext buildQuery(final int start, 
                                 final int end, 
                                 final QueryContext context, 
                                 final QuerySink sink) {
    final SemanticQuery.Builder builder = config.getBaselineQuery()
        .toBuilder()
        // TODO - PADDING compute the padding
        .setStart(Integer.toString(start - 300))
        .setEnd(Integer.toString(end));
    
    System.out.println("  BASELINE Q: " + JSON.serializeToString(builder.build()));
    return SemanticQueryContext.newBuilder()
        .setTSDB(context.tsdb())
        .setLocalSinks((List<QuerySink>) Lists.newArrayList(sink))
        .setQuery(builder.build())
        .setStats(context.stats())
        .setAuthState(context.authState())
        .setHeaders(context.headers())
        .build();
  }
  
  ChronoUnit modelDuration() {
    return model_units;
  }
  
  long predictionStart() {
    return prediction_start;
  }
  
  long predictionIntervals() {
    return prediction_intervals;
  }
  
  long predictionInterval() {
    return prediction_interval;
  }
  
  int jitter() {
    if (modelDuration() == ChronoUnit.DAYS) {
      // 1 hour jitter on 1m
      return (int) Math.abs(context.query().buildHashCode().asLong() % 59) * 60;
    } else {
      // 5m jitter on 15s
      return (int) Math.abs(context.query().buildHashCode().asLong() % 20) * 15;
    }
  }
  
  void writeCache(final QueryResult result) {
    context.tsdb().getQueryThreadPool().submit(new Runnable() {
      public void run() {
        class CacheErrorCB implements Callback<Object, Exception> {
          @Override
          public Object call(final Exception e) throws Exception {
            LOG.warn("Failed to cache EGADs prediction", e);
            return null;
          }
        }
        
        class SuccessCB implements Callback<Object, Void> {
          @Override
          public Object call(final Void ignored) throws Exception {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Successfully cached EGADs prediction at " 
                  + Arrays.toString(cache_key));
            }
            return null;
          }
        }
        
        final long expiration;
        if (model_units == ChronoUnit.HOURS) {
          expiration = 3600 * 2 * 1000;
        } else {
          expiration = 86400 * 2 * 1000;
        }
        cache.cache(cache_key, expiration, result, null)
          .addCallback(new SuccessCB())
          .addErrback(new CacheErrorCB());
      }
    });
  }
}

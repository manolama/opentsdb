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
import net.opentsdb.query.egads.EgadsResult;
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
  private final BaselineQuery[] baseline_queries;
  private final PredictionCache cache;
  private final CountDownLatch latch;
  private final int jitter;
  private final TemporalAmount jitter_duration;
  protected final AtomicBoolean failed;
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
    cache = null; // PULL FROM FACTORY
    this.config = config;
    baseline_queries = new BaselineQuery[config.getBaselineNumPeriods()];
    latch = new CountDownLatch(2);
    jitter = jitter();
    jitter_duration = Duration.ofSeconds(jitter);
    failed = new AtomicBoolean();
    
    long span = DateTime.parseDuration(config.getBaselinePeriod()) / 1000;
    if (span < 86400) {
      model_units = ChronoUnit.HOURS;
    } else {
      model_units = ChronoUnit.DAYS;
    }
    
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
    
    final long delta = context.query().endTime().msEpoch() - 
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
      String interval = DownsampleFactory.getAutoInterval(delta, 
          ((DownsampleFactory) factory).intervals(), null);
      prediction_interval = DateTime.parseDuration(interval) / 1000;
      prediction_intervals = delta / (prediction_interval * 1000);
    } else {
      prediction_interval = DateTime.parseDuration(ds.getInterval()) / 1000;
      prediction_intervals = delta / (prediction_interval * 1000);
    }
    
    final TimeStamp start = context.query().startTime().getCopy();
    final ChronoUnit duration = modelDuration();
    start.snapToPreviousInterval(1, duration);
//    if (jitter > 0) {
//      start.add(jitter_duration);
//    }
    prediction_start = start.epoch();
 // TODO - make this configurable and flexible.
    
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
    System.out.println("    GOT CURRENT: " + next.dataSource());
    current = next;
    countdown();
  }
  
  void run() {
    // Got baseline and current data, yay!
    final TLongObjectMap<Pair<TimeSeries, TimeSeries>> map = 
        new TLongObjectHashMap<Pair<TimeSeries, TimeSeries>>();
    
    for (final TimeSeries series : current.timeSeries()) {
      final long hash = series.id().buildHashCode();
      map.put(hash, new Pair<>(series, null));
    }
    
    for (final TimeSeries series : prediction.timeSeries()) {
      final long hash = ((EgadsTimeSeries) series).originalHash();
      Pair<TimeSeries, TimeSeries> pair = map.get(hash);
      if (pair != null) {
        pair.setValue(series);
      }
    }
    
    List<TimeSeries> time_series = Lists.newArrayList();
    TLongObjectIterator<Pair<TimeSeries, TimeSeries>> iterator = map.iterator();
    while (iterator.hasNext()) {
      iterator.advance();
      runPair(iterator.value());
    }
    
    if (config.getSerializeObserved()) {
      // yeah, ew, but it's an EgadsResult so we have an array list.
      prediction.timeSeries().addAll(current.timeSeries());
    }
    sendUpstream(prediction);
  }
  
  void runPair(final Pair<TimeSeries, TimeSeries> pair) {
    if (pair.getValue() == null) {
      return;
    }
    
    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> cur_op = 
        pair.getKey().iterator(NumericArrayType.TYPE);
    if (!cur_op.isPresent()) {
      cur_op = pair.getKey().iterator(NumericType.TYPE);
    }
    if (!cur_op.isPresent()) {
      LOG.warn("Nothing in current?!?!?!");
      return;
    }
    
    Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> pred_op = 
        pair.getValue().iterator(NumericArrayType.TYPE);
    if (!pred_op.isPresent()) {
      pred_op = pair.getValue().iterator(NumericType.TYPE);
    }
    if (!pred_op.isPresent()) {
      LOG.warn("Nothing in prediction?!?!?!");
      return;
    }
    
    ThresholdEvaluator eval;
    TypedTimeSeriesIterator<? extends TimeSeriesDataType> cur_it = cur_op.get();
    TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it = pred_op.get();
    final PredictIterator p;
    if (pred_it.getType() == NumericType.TYPE) {
      p = new NumPredIt(pred_it);
    } else {
      p = new ArrayPredIt(pred_it);
    }
    
    List<AlertValue> results = Lists.newArrayList();
    if (cur_it.hasNext()) {
      if (cur_it.getType() == NumericType.TYPE) {
        System.out.println("     CUR: NTYPE");
        eval = new ThresholdEvaluator(
            config.getUpperThreshold(),
            config.isUpperIsScalar(),
            config.getLowerThreshold(),
            config.isLowerIsScalar(),
            config.getSerializeThresholds() ? /* TODO */ 4096: 0);
        while (cur_it.hasNext()) {
          final TimeSeriesValue<NumericType> value = 
              (TimeSeriesValue<NumericType>) cur_it.next();
          // TODO - align data, etc.
          System.out.println("         CUR: " + value.timestamp() + "  " + value.value().toDouble());
          if (p.hasNext(value.timestamp())) {
            AlertValue a = eval.eval(value.timestamp(), value.value().toDouble(), p.value());
            if (a != null) {
              results.add(a);
            }
          }
        }
      } else {
        final TimeStamp ts = new SecondTimeStamp(prediction_start);
        System.out.println("     PRED START: " + ts);
        final TimeSeriesValue<NumericArrayType> value = 
            (TimeSeriesValue<NumericArrayType>) cur_it.next();
        eval = new ThresholdEvaluator(
            config.getUpperThreshold(),
            config.isUpperIsScalar(),
            config.getLowerThreshold(),
            config.isLowerIsScalar(),
            config.getSerializeThresholds() ? value.value().end() : 0);
        int idx = value.value().offset();
        while (idx < value.value().end()) {
          // TODO - align data, etc.
          AlertValue a = null;
          if (p.hasNext(ts)) {
          if (value.value().isInteger()) {
            a = eval.eval(ts, value.value().longArray()[idx], p.value()); 
          } else {
            a = eval.eval(ts, value.value().doubleArray()[idx], p.value());
          }
          
          if (a != null) {
            results.add(a);
          }
          }
          idx++;
          ts.add(Duration.ofSeconds(60));
        }
      }
    }
    
    if (results != null) {
      ((EgadsTimeSeries) pair.getValue()).addAlerts(results);
    }
    
    return;
  }
  
  interface PredictIterator {
    boolean hasNext(final TimeStamp expected);
    double value();
  }
  
  class NumPredIt implements PredictIterator {
    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it;
    TimeSeriesValue<NumericType> value;
    
    NumPredIt(final TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it) {
      if (pred_it.hasNext()) {
        this.pred_it = pred_it;
        value = (TimeSeriesValue<NumericType>) pred_it.next();
      } else {
        this.pred_it = null;
      }
    }
    
    public boolean hasNext(final TimeStamp expected) {
      if (pred_it == null || value == null) {
        return false;
      }
      
      if (value.timestamp().compare(Op.EQ, expected)) {
        return true;
      }
      
      while (value != null && value.timestamp().compare(Op.LT, expected)) {
        if (pred_it.hasNext()) {
          value = (TimeSeriesValue<NumericType>) pred_it.next();
        } else {
          value = null;
        }
      }
      
      if (value != null) {
        return true;
      } else {
        return false;
      }
    }
    
    public double value() {
      return value.value().toDouble();
    }
  }
  
  class ArrayPredIt implements PredictIterator {
    TimeStamp timestamp;
    TimeSeriesValue<NumericArrayType> value;
    int idx;
    
    ArrayPredIt(final TypedTimeSeriesIterator<? extends TimeSeriesDataType> pred_it) {
      System.out.println("      INSTANT ARRAY PRED IT: " + pred_it.hasNext());
      if (pred_it.hasNext()) {
        timestamp = new SecondTimeStamp(prediction_start);
        value = (TimeSeriesValue<NumericArrayType>) pred_it.next();
        idx = value.value().offset();
      }
    }
    
    public boolean hasNext(final TimeStamp expected) {
      if (timestamp == null) {
        return false;
      }
      
      if (idx > value.value().end()) {
        return false;
      }
      
      if (timestamp.compare(Op.EQ, expected)) {
        return true;
      }
      
      while (timestamp.compare(Op.LT, expected)) {
        idx++;
        // TODO - no hard code
        timestamp.add(Duration.ofSeconds(60));
        if (timestamp.compare(Op.EQ, expected)) {
          return true;
        }
      }
      
      return false;
    }
    
    public double value() {
      
      if (value.value().isInteger()) {
        return value.value().longArray()[idx];
      } else {
        return value.value().doubleArray()[idx];
      }
    }
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
     properties.setProperty("WINDOW_SIZE", "1");
    //       properties.setProperty("WINDOW_SIZE_UNITS", 
    //           DateTime.getDurationChronoUnits(modelSpan).toString());
     properties.setProperty("WINDOW_SIZE_UNITS", modelDuration().toString());
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
     properties.setProperty("PERIOD", modelDuration() == ChronoUnit.HOURS ? "3600" : "86400");
     
    
    List<TimeSeries> computed = Lists.newArrayList();
    TLongObjectIterator<OlympicScoringBaseline> it = join.iterator();
    while (it.hasNext()) {
      it.advance();
      TimeSeries ts = it.value().predict(properties);
      if (ts != null) {
        computed.add(ts);
      } else {
        System.out.println(" ------- NULL SERIES!");
      }
    }
    TimeStamp start = new SecondTimeStamp(prediction_start);
    TimeStamp end = start.getCopy();
    end.add(modelDuration() == ChronoUnit.HOURS ? Duration.ofHours(1) : Duration.ofDays(1));

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
    final TimeStamp start = context.query().startTime().getCopy();
    final ChronoUnit duration = modelDuration();
    start.snapToPreviousInterval(1, duration);
//    if (jitter > 0) {
//      start.add(jitter_duration);
//    }
    final TemporalAmount period = DateTime.parseDuration2(config.getBaselinePeriod());
    // advance to the oldest time first
    for (int i = 0; i < config.getBaselineNumPeriods(); i++) {
      start.subtract(period);
    }
    final TimeStamp end = start.getCopy();
    end.add(duration == ChronoUnit.DAYS ? Duration.ofDays(1) : Duration.ofHours(1));
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
      start.add(duration == ChronoUnit.DAYS ? Duration.ofDays(1) : Duration.ofHours(1));
      end.add(duration == ChronoUnit.DAYS ? Duration.ofDays(1) : Duration.ofHours(1));
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
}

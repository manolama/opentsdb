// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.downsample;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.opentsdb.data.TypedTimeSeriesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.exceptions.QueryUpstreamException;
import net.opentsdb.data.ZonedNanoTimeStamp;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.processor.ProcessorFactory;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;

/**
 * A processing node that performs downsampling on each individual time series
 * passed in as a result.
 * 
 * @since 3.0
 */
public class Downsample extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(Downsample.class);
  
  /** The non-null config. */
  private final DownsampleConfig config;
  
  protected final long interval_ms;
  
  protected final boolean use_calendar;
  
  // set size layout: 0 => min interval, 1 => max interval, 2 => intervals, 3+ => ?
  protected Map<String, long[]> set_sizes;
  
  protected Map<String, AtomicReferenceArray<DownsamplePartialTimeSeriesSet>> sets;
  
  protected boolean aligned;
  
  protected NumericAggregator aggregator;
  
  /**
   * Default ctor.
   * @param factory A non-null {@link DownsampleFactory}.
   * @param context A non-null context.
   * @param config A non-null {@link DownsampleConfig}.
   */
  public Downsample(final QueryNodeFactory factory, 
                    final QueryPipelineContext context,
                    final DownsampleConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null.");
    }
    this.config = config;
    interval_ms = DateTime.parseDuration(config.getInterval());
    use_calendar = config.timezone() != null && 
        !config.timezone().equals(Const.UTC);
    set_sizes = Maps.newConcurrentMap();
    sets = Maps.newConcurrentMap();
    NumericAggregatorFactory agg_factory = context.tsdb()
        .getRegistry().getPlugin(NumericAggregatorFactory.class, 
            config.getAggregator());
    aggregator = agg_factory.newAggregator(config.getInfectiousNan());
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    return super.initialize(span);
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  @Override
  public void close() {
    // No-op
  }
  
  @Override
  public void onNext(final QueryResult next) {
    final DownsampleResult results = new DownsampleResult(next);
    for (final QueryNode us : upstream) {
      try {
        us.onNext(results);
      } catch (Exception e) {
        LOG.error("Failed to call upstream onNext on Node: " + us, e);
        results.close();
      }
    }
  }
  
  @Override
  public void onNext(final PartialTimeSeries pts) {
    final String source = pts.set().dataSource();
    long[] sizes = set_sizes.get(source);
    if (sizes == null) {
      boolean matched = false;
      for (final TimeSeriesDataSource src : downstream_sources) {
        if (pts.set().dataSource().equals(src.config().getId())) {
          matched = true;
          break;
        }
      }
      
      if (!matched) {
        // not one of ours.
        return;
      }
      
      try {
        sizes = initSet(pts);
      } catch (Throwable t) {
        LOG.error("Failed to initialize source: " + source);
        sendUpstream(t);
        return;
      }
    }
    
    final AtomicReferenceArray<DownsamplePartialTimeSeriesSet> source_sets = 
        sets.get(source);
    if (source_sets == null) {
      LOG.error("No set array found for: " + source);
      this.sendUpstream(new IllegalStateException("No set array found for: " + source));
      return;
    }
    
    final long start = pts.set().start().epoch();
    int idx = (int) ((start - sizes[3]) / (sizes[1] / 1000));
    System.out.println("          ST: " + start + "  s3: " + sizes[3] + "  s1: " + sizes[1] + "  IDX: " + idx + "  SSL: " + source_sets.length());
    if (idx >= source_sets.length() || idx < 0) {
      LOG.warn("Dropping time series that didn't match a segment. PTS "
          + "start time: " + pts.set().start().epoch());
      return;
    }
    
    DownsamplePartialTimeSeriesSet set = source_sets.get(idx);
    if (set == null) {
      set = (DownsamplePartialTimeSeriesSet) context.tsdb().getRegistry()
          .getObjectPool(DownsamplePartialTimeSeriesSetPool.TYPE).claim().object();
      if (source_sets.compareAndSet(idx, null, set)) {
        set.reset(this, source, idx);
      } else {
        // lost the race
        try {
          set.close();
        } catch (Exception e) {
          LOG.error("Failed to put set back in the downsample set pool.");
        }
        set = source_sets.get(idx);
      }
    }
    set.process(pts);
    
    // now we have to see if our incoming set spans multiple local sets.
    while (++idx < source_sets.length() && 
           pts.set().end().compare(Op.GT, set.end())) {
      System.out.println(" ADDING pts to another sert at idx " + idx);
      set = source_sets.get(idx);
      if (set == null) {
        set = (DownsamplePartialTimeSeriesSet) context.tsdb().getRegistry()
            .getObjectPool(DownsamplePartialTimeSeriesSetPool.TYPE).claim().object();
        if (source_sets.compareAndSet(idx, null, set)) {
          set.reset(this, source, idx);
        } else {
          // lost the race
          try {
            set.close();
          } catch (Exception e) {
            LOG.error("Failed to put set back in the downsample set pool.");
          }
          set = source_sets.get(idx);
        }
      }
      set.process(pts);
    }
    
    // TODO - free the pts
  }
  
  /**
   * Three important things to consider here:
   * <ol>
   * <li>The sizes of segments from the data source. Usually 1h, though may be
   * greater like 1 day for rollup data.</li>
   * <li>The actual set size from the given pts. This could be from another node
   * that returns a size greater than that of the source max. E.g. another
   * downsample node.</li>
   * <li>The downsample interval. Some cases could cause us to return one big
   * segment, like 33m.</li>
   * </ol>
   * @param pts
   * @return
   */
  protected synchronized long[] initSet(final PartialTimeSeries pts) {
    System.out.println("   DS: " + downstream_sources.size());
    final String data_source = pts.set().dataSource();
    TimeSeriesDataSource source = null;
    for (final TimeSeriesDataSource src : downstream_sources) {
      if (data_source.equals(src.config().getId())) {
        source = src;
        break;
      }
    }
    
    if (source == null) {
      // not one of ours.
      return null;
    }
    
    System.out.println("[[SRC]]: " + System.identityHashCode(this));
    
    // shortcut if we have an empty result.
    if (pts.set().totalSets() == 1) {
      long[] sizes = new long[4];
      sizes[0] = interval_ms;
      sizes[1] = (pts.set().end().msEpoch() - pts.set().start().msEpoch());
      sizes[2] = 1;
      sizes[3] = pts.set().start().epoch();
      AtomicReferenceArray<DownsamplePartialTimeSeriesSet> extant = 
          sets.putIfAbsent(source.config().getId(), new AtomicReferenceArray<DownsamplePartialTimeSeriesSet>(1));
      if (extant == null) {
        long[] extant_sizes = set_sizes.putIfAbsent(source.config().getId(), sizes);
        if (extant_sizes != null) {
          LOG.warn("Still lost a race initializating a set for source: " 
              + source.config().getId());
          return extant_sizes;
        }
      }
      return sizes;
    }
    
    // else we have multiple intervals and need to compute the width we'll use
    // for our segments.
    String max_string = config.getInterval();
    long max = interval_ms;
    long min = Long.MAX_VALUE;
    final String[] set_intervals = source.setIntervals();
    if (set_intervals == null) {
      throw new IllegalStateException("Source " + source.config().getId() 
          + " returned a null set intervals.");
    }
    for (int i = 0; i < set_intervals.length; i++) {
      long interval = DateTime.parseDuration(set_intervals[i]);
      System.out.println("       SET INT: " + set_intervals[i] + "  I " + interval);
      if (interval < min) {
        min = interval;
      }
      if (interval > max) {
        max_string = set_intervals[i];
        max = interval;
      }
    }
    
    long config_interval = interval_ms;
    if (config_interval < min) {
      min = config_interval;
    }

    // TODO - see if we want to just set the max to the segment size. Otherwise
    // we may be splitting segments into smaller chunks.
//    if ((pts.set().end().msEpoch() - pts.set().start().msEpoch()) > max) {
//      // the given segment was actually larger so we *may* want to use that
//      // instead.
//    }
    System.out.println("       MAX: " + max);
    
    // TODO - calendar
    final int amt = DateTime.getDurationInterval(max_string);
    final String u = DateTime.getDurationUnits(max_string);
    System.out.println("  UNITS: " + u + "  AMT: " + amt + "  INT: " + config.interval());
    TimeStamp st = source.firstSetStart().getCopy();
    TimeStamp end = new SecondTimeStamp(context.query().endTime().epoch());
    if (u.toLowerCase().equals("h")) {
      end.snapToPreviousInterval(amt, ChronoUnit.HOURS);
    } else if (u.toLowerCase().equals("d")) {
      end.snapToPreviousInterval(amt, ChronoUnit.DAYS); 
    } else {
      throw new IllegalStateException("Unsupported source interval "
          + "unit: " + u);
    }
    
    final TemporalAmount duration = DateTime.parseDuration2(max_string);
    if (end.compare(Op.LT, config.endTime())) {
      // we need at least one more segment.
      end.add(duration);
    }
    
    long num_intervals = (end.msEpoch() - st.msEpoch()) / max;
    if (num_intervals <= 0) {
      // in case the segment size is larger than the query size.
      num_intervals = 1;
    }
    
    System.out.println("CFG: " + config.startTime().epoch() + "  SNAP " + st.epoch() + " EN: " + end.epoch() + " D " + (end.epoch() - st.epoch()));
    if (interval_ms > 0 && max % interval_ms != 0) {
      // crap. We may have a weird interval lik 45m on top of 1h segments
      // where we need to span multiple segments.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Odd interval: " + config.getInterval() 
          + " for downsampling.");
      }
      
      final long start_ts = source.firstSetStart().msEpoch();
      long end_ts = start_ts + max;
      while ((end_ts - start_ts) % interval_ms != 0 && end_ts < end.msEpoch()) {
        end_ts += max;
      }
      
      if (end_ts >= end.msEpoch()) {
        // we'll just return one big segment, *sniff*
        System.out.println("JUST ONE INTERVAL " + end_ts + "  QE: " + end.msEpoch());
        max = (end_ts - start_ts);
        num_intervals = 1;
      } else {
        // we can break it up. The last segment may be oddly shaped.
        max = (end_ts - start_ts);
        num_intervals = (end.msEpoch() - st.msEpoch()) / max;
        if (num_intervals <= 0) {
          // in case the segment size is larger than the query size.
          num_intervals = 1;
        }
      }
      System.out.println("NEW interval = " + ((end_ts - start_ts) / 1000) +"s");
      
    }
    
    // TODO - we may want some logic to figure a more optimal size in
    // the event we have rollups and fall back to raw, etc.
    long[] sizes = new long[3 + (int) num_intervals];
    sizes[0] = min;
    sizes[1] = max;
    sizes[2] = num_intervals;
    
    for (int i = 0; i < num_intervals; i++) {
      System.out.println("        TS: " + st);
      sizes[i + 3] = st.epoch();
      st.add(duration);
    }
    
    System.out.println("  SETS: " + Arrays.toString(sizes) + " FOR: " + source.config().getId());
    // WARNING: Ordering is super important here.
    AtomicReferenceArray<DownsamplePartialTimeSeriesSet> extant = 
        sets.putIfAbsent(source.config().getId(), new AtomicReferenceArray<DownsamplePartialTimeSeriesSet>((int) num_intervals));
    if (extant == null) {
      long[] extant_sizes = set_sizes.putIfAbsent(source.config().getId(), sizes);
      if (extant_sizes != null) {
        LOG.warn("Still lost a race initializating a set for source: " 
            + source.config().getId());
        return extant_sizes;
      }
    } else {
      // lost the race on the ARA.
    }
    System.out.println("-----------");

    return sizes;
  }
  
  protected long[] getSizes(final String source) {
    return set_sizes.get(source);
  }
  
  protected void sendUpstream(final PartialTimeSeries series) 
      throws QueryUpstreamException {
    super.sendUpstream(series);
  }
  
  /**
   * A downsample result that's a member class of the main node so that we share
   * the references to the config and node.
   */
  class DownsampleResult extends BaseWrappedQueryResult 
      implements TimeSpecification {
    /** Countdown latch for closing the result set based on the upstreams. */
    private final CountDownLatch latch;
    
    /** The new downsampler time series applied to the result's time series. */
    private final List<TimeSeries> downsamplers;
    
    /** The downsampled resolution. */
    private final ChronoUnit resolution;
    
    /** The snapped starting timestamp of the downsample span. */
    private final TimeStamp start;
    
    /** The snapped final timestamp of the downsample span. */
    private final TimeStamp end;
    
    /**
     * Default ctor that dumps each time series into a new Downsampler time series.
     * @param results The non-null results set.
     */
    DownsampleResult(final QueryResult results) {
      super(results);
      latch = new CountDownLatch(Downsample.this.upstream.size());
      downsamplers = Lists.newArrayListWithCapacity(results.timeSeries().size());
      for (final TimeSeries series : results.timeSeries()) {
        downsamplers.add(new DownsampleTimeSeries(series));
      }
      if (config.units() == null) {
        resolution = ChronoUnit.FOREVER;
      } else {
        switch (config.units()) {
        case NANOS:
        case MICROS:
          resolution = ChronoUnit.NANOS;
          break;
        case MILLIS:
          resolution = ChronoUnit.MILLIS;
          break;
        default:
          resolution = ChronoUnit.SECONDS;
        }
      }
      
      final SemanticQuery query = (SemanticQuery) context.query();
      if (config.getRunAll()) {
        start = query.startTime();
        end = query.endTime();
      } else if (config.timezone() != Const.UTC) {
        start = new ZonedNanoTimeStamp(query.startTime().epoch(), 
            query.startTime().nanos(), config.timezone());
        start.snapToPreviousInterval(config.intervalPart(), config.units());
        if (start.compare(Op.LT, query.startTime())) {
          nextTimestamp(start);
        }
        end = new ZonedNanoTimeStamp(query.endTime().epoch(), 
            query.endTime().nanos(), config.timezone());
        end.snapToPreviousInterval(config.intervalPart(), config.units());
        if (end.compare(Op.LTE, start)) {
          throw new IllegalArgumentException("Snapped end time: " + end 
              + " must be greater than the start time: " + start);
        }
      } else {
        start = query.startTime().getCopy();
        start.snapToPreviousInterval(config.intervalPart(), config.units());
        if (start.compare(Op.LT, query.startTime())) {
          nextTimestamp(start);
        }
        end = query.endTime().getCopy();
        end.snapToPreviousInterval(config.intervalPart(), config.units());
        if (end.compare(Op.LTE, start)) {
          throw new IllegalArgumentException("Snapped end time: " + end 
              + " must be greater than the start time: " + start);
        }
      }
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      return config.getFill() ? this : result.timeSpecification();
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return downsamplers;
    }
    
    @Override
    public QueryNode source() {
      return Downsample.this;
    }
    
    @Override
    public ChronoUnit resolution() {
      return resolution;
    }
    
    public void updateTimestamp(final int offset, final TimeStamp timestamp) {
      if (offset < 0) {
        throw new IllegalArgumentException("Negative offsets are not allowed.");
      }
      if (timestamp == null) {
        throw new IllegalArgumentException("Timestamp cannot be null.");
      }
      if (config.getRunAll()) {
        timestamp.update(start);
      } else {
        final TimeStamp increment = new ZonedNanoTimeStamp(
            start.epoch(), start.msEpoch(), start.timezone());
        for (int i = 0; i < offset; i++) {
          increment.add(config.interval());
        }
        timestamp.update(increment);
      }
    }
    
    public void nextTimestamp(final TimeStamp timestamp) {
      if (config.getRunAll()) {
        timestamp.update(start);
      } else {
        timestamp.add(config.interval());
      }
    }
    
    @Override
    public TimeStamp start() {
      return start;
    }
    
    @Override
    public TimeStamp end() {
      return end;
    }
    
    @Override
    public TemporalAmount interval() {
      return config.interval();
    }

    @Override
    public String stringInterval() {
      return config.getInterval();
    }
    
    @Override
    public ChronoUnit units() {
      return config.units();
    }

    @Override
    public ZoneId timezone() {
      return config.timezone();
    }
    
    /** @return The downstream results. */
    QueryResult downstreamResult() {
      return result;
    }
    
    /**
     * The super simple wrapper around the time series source that generates 
     * iterators using the factory.
     */
    class DownsampleTimeSeries implements TimeSeries {
      /** The non-null source. */
      private final TimeSeries source;
      
      /**
       * Default ctor.
       * @param source The non-null source to pull data from.
       */
      private DownsampleTimeSeries(final TimeSeries source) {
        this.source = source;
      }
      
      @Override
      public TimeSeriesId id() {
        return source.id();
      }

      @Override
      public Optional<TypedTimeSeriesIterator> iterator(
          final TypeToken<? extends TimeSeriesDataType> type) {
        if (type == null) {
          throw new IllegalArgumentException("Type cannot be null.");
        }
        if (!source.types().contains(type)) {
          return Optional.empty();
        }
        final TypedTimeSeriesIterator iterator = 
            ((ProcessorFactory) Downsample.this.factory()).newTypedIterator(
                type, 
                Downsample.this, 
                DownsampleResult.this,
                Lists.newArrayList(source));
        if (iterator != null) {
          return Optional.of(iterator);
        }
        return Optional.empty();
      }
      
      @Override
      public Collection<TypedTimeSeriesIterator> iterators() {
        final Collection<TypeToken<? extends TimeSeriesDataType>> types = source.types();
        final List<TypedTimeSeriesIterator> iterators =
            Lists.newArrayListWithCapacity(types.size());
        for (final TypeToken<? extends TimeSeriesDataType> type : types) {
          iterators.add(((ProcessorFactory) Downsample.this.factory()).newTypedIterator(
              type, 
              Downsample.this, 
              DownsampleResult.this,
              Lists.newArrayList(source)));
        }
        return iterators;
      }

      @Override
      public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
        // TODO - join with the factories supported.
        return source.types();
      }

      @Override
      public void close() {
        source.close();
      }
      
    }

  }
  
}

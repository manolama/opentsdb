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

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.opentsdb.data.TypedTimeSeriesIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
  
  protected long set_interval;
  
  // set size layout: 0 => min interval, 1 => max interval, 2 => intervals, 3+ => ?
  protected Map<String, long[]> set_sizes;
  
  protected Map<String, AtomicReferenceArray<DownsamplePartialTimeSeriesSet>> sets;
  
  protected boolean aligned;
  
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
  }
  
  @Override
  public Deferred<Void> initialize(final Span span) {
    class PushCB implements Callback<Void, Void> {
      @Override
      public Void call(final Void ignored) throws Exception {
        set_sizes = Maps.newHashMapWithExpectedSize(downstream_sources.size());
        for (final TimeSeriesDataSource source : downstream_sources) {
          long max = 0;
          long min = Long.MAX_VALUE;
          final String[] set_intervals = source.setIntervals();
          for (int i = 0; i < set_intervals.length; i++) {
            long interval = DateTime.parseDuration(set_intervals[i]);
            if (interval < min) {
              min = interval;
            } else if (interval < max) {
              max = interval;
            }
          }
          
          long config_interval = DateTime.parseDuration(config.getInterval());
          if (config_interval > min) {
            min = config_interval;
          }
          
          long num_intervals = (config.endTime().msEpoch() - 
              config.startTime().msEpoch()) / max;
          if (num_intervals <= 0) {
            // in case the segment size is larger than the query size.
            num_intervals = 1;
          }
          
          // TODO - we may want some logic to figure a more optimal size in
          // the event we have rollups and fall back to raw, etc.
          long[] sizes = new long[3 + (int) num_intervals];
          sizes[0] = min;
          sizes[1] = max;
          sizes[2] = num_intervals;
          
          final Duration duration = Duration.ofSeconds(max / 1000);
          if (use_calendar) {
            // TODO - snap
            // TODO - see if our calendar boundaries are nicely aligned
          } else {
            final TimeStamp ts = new SecondTimeStamp(
                config.startTime().msEpoch() - (config.startTime().msEpoch() % max));
            for (int i = 0; i < num_intervals; i++) {
              sizes[i + 2] = ts.epoch();
              ts.add(duration);
            }
          }
          
          set_sizes.put(source.config().getId(), sizes);
        }
        return null;
      }
    }
    
    // TODO - centralize this flag.
    if (context.tsdb().getConfig().hasProperty("tsd.storage.enable_push")) {
      return super.initialize(span).addCallback(new PushCB());
    }
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
      LOG.error("WTF? No sizes for: " + source);
      this.sendUpstream(new RuntimeException("GRR"));
    }
    
    final AtomicReferenceArray<DownsamplePartialTimeSeriesSet> source_sets = 
        sets.get(source);
    if (source_sets == null) {
      LOG.error("WTF? No sets for: " + source);
      this.sendUpstream(new RuntimeException("GRR"));
    }
    long start = pts.set().start().epoch();
    int idx = (int) ((start - sizes[3]) / sizes[2]);
    if (source_sets.get(idx) == null) {
      DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
      if (source_sets.compareAndSet(idx, null, set)) {
        set.reset(this, source, idx);
      } else {
        // lost the race
        set = source_sets.get(idx);
      }
      set.process(pts);
    }
    
    if (idx + 1 == sizes[2]) {
      return;
    }
    
    // see if we need to add to more sets.
    idx++;
    while (pts.set().end().epoch() > sizes[idx + 3] && idx < sizes[2]) {
      if (source_sets.get(idx) == null) {
        DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
        if (source_sets.compareAndSet(idx, null, set)) {
          set.reset(this, source, idx);
        } else {
          // lost the race
          set = source_sets.get(idx);
        }
        set.process(pts);
      }
      idx++;
    }
  }
  
  long[] getSizes(final String source) {
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

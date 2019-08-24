package net.opentsdb.query.execution.cache;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Pair;

public class CombinedResult implements QueryResult, TimeSpecification {

  Map<Long, TimeSeries> time_series;
  TimeStamp spec_start;
  TimeStamp spec_end;
  TimeSpecification spec;
  QueryNode node;
  String data_source;
  
  final QueryPipelineContext context;
  final List<QuerySink> sinks;
  final AtomicInteger latch;
  final int result_interval;
  final ChronoUnit result_units;
  
  public CombinedResult(final QueryPipelineContext context,
                        final QueryResult[] results, 
                        final List<QuerySink> sinks, 
                        final AtomicInteger latch,
                        final String result_interval) {
    this.context = context;
    this.sinks = sinks;
    this.latch = latch;
    this.result_interval = DateTime.getDurationInterval(result_interval);
    result_units = DateTime.getDurationUnits(result_interval).equals("h") ? ChronoUnit.HOURS : ChronoUnit.DAYS;
    time_series = Maps.newHashMap();
    for (int i = 0; i < results.length; i++) {
      if (results[i] == null) {
        continue;
      }
      
      if (spec_start == null && results[i].timeSpecification() != null) {
        spec_start = context.query().startTime();
        while (spec_start.compare(Op.LT, context.query().startTime())) {
          spec_start.add(results[i].timeSpecification().interval());
        }
        
        spec_end = context.query().endTime();
        while (spec_end.compare(Op.GT, context.query().endTime())) {
          spec_end.subtract(results[i].timeSpecification().interval());
        }
        System.out.println("         SPEC START: " + spec_start.epoch() + "  END: " + spec_end.epoch() + "  DIFF: " + (spec_end.epoch() - spec_start.epoch()));
      }
      
      node = results[i].source();
      data_source = results[i].dataSource();
      
      // TODO more time spec
      spec = results[i].timeSpecification();
      
      // TODO handle tip merge eventually
      for (final TimeSeries ts : results[i].timeSeries()) {
        final long hash = ts.id().buildHashCode();
        System.out.println("      ID HASH: " + hash);
        TimeSeries combined = time_series.get(hash);
        if (combined == null) {
          combined = new CombinedTimeSeries(this, results[i], ts);
          time_series.put(hash, combined);
        } else {
          ((CombinedTimeSeries) combined).series.add(new Pair<>(results[i], ts));
        }
      }
    }
    System.out.println("        TOTAL RESULTS: " + time_series.size());
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return spec_start == null ? null : this;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return time_series.values();
  }

  @Override
  public String error() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Throwable exception() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public String dataSource() {
    return data_source;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return ChronoUnit.SECONDS;
  }

  @Override
  public RollupConfig rollupConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    System.out.println("-------- CLOSING");
    if (latch.decrementAndGet() == 0) {
      for (final QuerySink sink : sinks) {
        sink.onComplete();
      }
    }
//    for (final TimeSeries ts : time_series.values()) {
//      ts.close();
//    }
  }

  @Override
  public TimeStamp start() {
    return spec_start;
  }

  @Override
  public TimeStamp end() {
    return spec_end;
  }

  @Override
  public TemporalAmount interval() {
    return spec.interval();
  }

  @Override
  public String stringInterval() {
    return spec.stringInterval();
  }

  @Override
  public ChronoUnit units() {
    return spec.units();
  }

  @Override
  public ZoneId timezone() {
    return spec.timezone();
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    spec.updateTimestamp(offset, timestamp);
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    spec.nextTimestamp(timestamp);
  }

  int resultInterval() {
    return result_interval;
  }
  
  ChronoUnit resultUnits() {
    return result_units;
  }
}

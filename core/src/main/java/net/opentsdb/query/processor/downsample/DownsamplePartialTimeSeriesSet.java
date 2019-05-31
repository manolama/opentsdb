package net.opentsdb.query.processor.downsample;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Maps;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.query.QueryNode;

public class DownsamplePartialTimeSeriesSet implements PartialTimeSeriesSet {
  protected Downsample node;
  TimeStamp start = new SecondTimeStamp(0L);
  TimeStamp end = new SecondTimeStamp(0L);
  int total_sets;
  String source;
  long interval;
  // TODO - different data types
  Map<Long, DownsamplePartialTimeSeries> timeseries = Maps.newConcurrentMap();
  
  void reset(final Downsample node, final String source, final int idx) {
    this.node = node;
    this.source = source;
    final long[] sizes = node.set_sizes.get(source);
    interval = sizes[0];
    total_sets = (int) sizes[1];
    start.updateEpoch(sizes[idx + 2]);
    end.update(start);
    
    if (node.use_calendar) {
      // TODO - 
    } else {
      end.add(Duration.ofSeconds(sizes[1] / 1000));
    }
  }
  
  void process(final PartialTimeSeries series) {
    boolean multiples = (series.set().end().msEpoch() - 
        series.set().start().msEpoch()) < interval;
    
    // TODO - compute sets
    
    if (series.value() == null) {
      // TODO
    } else {
      DownsamplePartialTimeSeries pts = timeseries.get(series.idHash());
      if (pts == null) {
        if (series.value().type() == NumericLongArrayType.TYPE) {
          pts = new DownsampleNumericPartialTimeSeries(null); // TODO - pool
          DownsamplePartialTimeSeries extant = timeseries.putIfAbsent(series.idHash(), pts);
          if (extant != null) {
            pts = extant;
          } else {
            pts.reset(node, this, multiples);
          }
        } else {
          throw new RuntimeException("Unhandled type: " + series.value().type());
        }
      }
      
      pts.addSeries(series);
    }
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int totalSets() {
    return total_sets;
  }

  @Override
  public boolean complete() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public QueryNode node() {
    return node;
  }

  @Override
  public String dataSource() {
    return source;
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
  public TimeSeriesId id(long hash) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int timeSeriesCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }
}

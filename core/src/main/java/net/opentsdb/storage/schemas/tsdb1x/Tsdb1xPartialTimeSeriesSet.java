package net.opentsdb.storage.schemas.tsdb1x;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;

public class Tsdb1xPartialTimeSeriesSet implements PartialTimeSeriesSet {

  QueryNode node;
  TimeStamp start;
  TimeStamp end;
  int series;
  int latch;
  boolean complete;
  int total_sets;
  TLongObjectMap<TimeSeriesId> ids;
  
  public Tsdb1xPartialTimeSeriesSet() {
    start = new SecondTimeStamp(0);
    end = new SecondTimeStamp(0);
  }
  
  public void reset(final QueryNode node, final TimeStamp start, final TimeStamp end, final int salts, final int total_sets, final TLongObjectMap<TimeSeriesId> ids) {
    this.node = node;
    this.start.update(start);
    this.end.update(end);
    series = 0;
    complete = false;
    latch = salts;
    this.total_sets = total_sets;
    this.ids = ids;
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
  public synchronized boolean complete() {
    return complete;
  }

  @Override
  public QueryNode node() {
    return node;
  }

  @Override
  public String dataSource() {
    return node.config().getId();
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
    synchronized (ids) {
      return ids.get(hash);
    }
  }

  @Override
  public int timeSeriesCount() {
    return series;
  }

  @Override
  public TimeSpecification timeSpecification() {
    // for now always null.
    return null;
  }

  public synchronized void setCompleteAndEmpty() {
    latch--;
    if (latch == 0) {
      complete = true;
    }
  }
  
  public synchronized void increment(final boolean complete) {
    series++;
    if (complete) {
      latch--;
      if (latch == 0) {
        this.complete = true;
      }
    }
  }
}

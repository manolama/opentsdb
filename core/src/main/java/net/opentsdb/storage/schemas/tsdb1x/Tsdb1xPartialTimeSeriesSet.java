package net.opentsdb.storage.schemas.tsdb1x;

import java.util.concurrent.atomic.AtomicReference;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.NoDataPTSPool;
import net.opentsdb.query.QueryNode;

public class Tsdb1xPartialTimeSeriesSet implements PartialTimeSeriesSet {

  final TSDB tsdb;
  QueryNode node;
  TimeStamp start;
  TimeStamp end;
  int series;
  public int latch;
  boolean complete;
  int total_sets;
  TLongObjectMap<TimeSeriesId> ids;
  AtomicReference<Tsdb1xPartialTimeSeries> pts;
  
  
  public Tsdb1xPartialTimeSeriesSet(final TSDB tsdb) {
    this.tsdb = tsdb;
    pts = new AtomicReference<Tsdb1xPartialTimeSeries>();
    series = 0;
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
    //System.out.println(" ######## Latch: " + latch);
    if (--latch == 0) {
      complete = true;
    }
    if (complete) {
      Tsdb1xPartialTimeSeries extant = this.pts.getAndSet(null);
      if (extant != null) {
        // TODO - pool
        System.out.println(" ---------- push non null FROM EMPTY pts: " + extant);
        PooledPSTRunnable runnable = new PooledPSTRunnable();
        runnable.reset(extant, node);
        tsdb.getQueryThreadPool().submit(runnable);
      } else {
        //System.out.println(" ------------- push empty pts");
        // send up sentinel
        NoDataPartialTimeSeries pts = (NoDataPartialTimeSeries) 
            tsdb.getRegistry().getObjectPool(
                NoDataPTSPool.TYPE).claim().object();
        pts.reset(this);
        // TODO - pool
        PooledPSTRunnable runnable = new PooledPSTRunnable();
        runnable.reset(pts, node);
        tsdb.getQueryThreadPool().submit(runnable);
      }
    }
  }
  
  public void increment(Tsdb1xPartialTimeSeries pts, final boolean complete) {
    boolean all_done = false;
    synchronized (this) {
      //System.out.println("---------- incrementing... " + complete);
      series++;
      if (complete) {
        //System.out.println(" ######## Latch: " + latch);
        if (--latch == 0) {
          this.complete = true;
          all_done = true;
        }
      }
    }
    
    Tsdb1xPartialTimeSeries extant = this.pts.getAndSet(pts);
    if (extant != null) {
      // TODO - pool
      System.out.println(" ---------- push non null EXANT pts: " + start.epoch());
      PooledPSTRunnable runnable = new PooledPSTRunnable();
      runnable.reset(extant, node);
      tsdb.getQueryThreadPool().submit(runnable);
    }
    
    if (all_done) {
      // TODO - pool
      System.out.println(" ---------- push non null NEW pts: " + start.epoch());
      PooledPSTRunnable runnable = new PooledPSTRunnable();
      runnable.reset(pts, node);
      tsdb.getQueryThreadPool().submit(runnable);
      this.pts.set(null);
    }
  }
}

package net.opentsdb.storage.schemas.tsdb1x;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.query.QueryNode;

public class PooledPSTRunnable implements Runnable {
  public PartialTimeSeries pts;
  public QueryNode node;
  
  public void reset(PartialTimeSeries pts, QueryNode node) {
    this.pts = pts;
    this.node = node;
  }
  
  @Override
  public void run() {
    try {
      //System.out.println("SENDING UP: " + pts);
      if (pts != null && pts instanceof Tsdb1xPartialTimeSeries) {
        ((Tsdb1xPartialTimeSeries) pts).dedupe(false, false);
      }
      node.onNext(pts);
    } catch (Throwable t) {
      t.printStackTrace();
      node.onError(t);
    }
  }

}

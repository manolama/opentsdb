package net.opentsdb.storage.schemas.tsdb1x;

import java.util.List;

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupUtils.RollupUsage;
import net.opentsdb.storage.SourceNode;

public abstract class BaseTsdb1xQueryNode implements TimeSeriesDataSource, SourceNode {

  public abstract List<RollupInterval> rollupIntervals();
  
  public abstract RollupUsage rollupUsage();
  
  public abstract void sentData();
  
}

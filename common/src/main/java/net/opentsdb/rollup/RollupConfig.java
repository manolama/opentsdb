package net.opentsdb.rollup;

import java.util.List;
import java.util.Map;

public interface RollupConfig {

  public RollupInterval getRollupInterval(final String interval);
  
  public List<RollupInterval> getRollupIntervals(final String interval);
  
  public List<RollupInterval> getRollupIntervals();
  
  public Map<String, RollupInterval> getRollups();
  
}

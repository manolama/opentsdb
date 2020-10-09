package net.opentsdb.data;

public interface HashedLowLevelTimeSeries extends LowLevelTimeSeries {
  /** The full time series hash. */
  long seriesHash();
  
  /** The hash of the tag set (remember it's sorted already) */
  long tagsSetHash();
  
  /** Hashes for the iterator over the tag set. */
  long tagPairHash();
  long tagKeyHash();
  long tagValueHash();
}

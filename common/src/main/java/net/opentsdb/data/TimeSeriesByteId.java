package net.opentsdb.data;

import net.opentsdb.storage.StorageSchema;

/**
 * TODO - This is just a native ID e.g. TSUID as bytes
 *
 */
public interface TimeSeriesByteId extends TimeSeriesId, Comparable<TimeSeriesByteId> {
  
  public StorageSchema schema();
  
  public byte[] alias();
  
  public byte[] namespace();
  
  public byte[] metric();
  
  // tag key value key value.... flat for space savings
  public byte[] tags();
  
  public byte[] tagValue(final byte[] key);
  
  // tag keys in flat array for saving space
  public byte[] aggregatedTags();
  
  // tag keys in flat array for space savings
  public byte[] disjointTags();
  
  public byte[] timeseriesUID();
}

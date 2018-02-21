package net.opentsdb.data;

import java.util.List;
import java.util.Set;

import net.opentsdb.storage.StorageSchema;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * TODO - This is just a native ID e.g. TSUID as bytes
 *
 */
public interface TimeSeriesByteId extends TimeSeriesId, Comparable<TimeSeriesByteId> {
  
  public StorageSchema schema();
  
  public byte[] alias();
  
  public byte[] namespace();
  
  public byte[] metric();
  
  public ByteMap<byte[]> tags();
    
  public List<byte[]> aggregatedTags();
  
  public List<byte[]> disjointTags();
  
  public Set<byte[]> uniqueIds();
}

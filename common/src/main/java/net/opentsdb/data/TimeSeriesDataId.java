package net.opentsdb.data;

import net.opentsdb.utils.Bytes.ByteMap;

public interface TimeSeriesDataId extends TimeSeriesId {

  public byte[] namespace();
  
  public byte[] metric();
  
  public ByteMap<byte[]> tags();
  
  public byte[] timeseriesUID();
}

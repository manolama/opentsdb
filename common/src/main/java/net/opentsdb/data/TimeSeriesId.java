package net.opentsdb.data;

import com.stumbleupon.async.Deferred;

public interface TimeSeriesId {
  /**
   * @return True if the fields are encoded using a format specified by the 
   * storage engine.
   */
  public boolean encoded();
  
  public boolean decodeToJoin();
  
  public Deferred<TimeSeriesId> decode();
}

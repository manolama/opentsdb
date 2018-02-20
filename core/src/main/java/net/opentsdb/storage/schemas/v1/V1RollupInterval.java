package net.opentsdb.storage.schemas.v1;

import net.opentsdb.rollup.RollupInterval;

public class V1RollupInterval implements RollupInterval {

  public long rollupBaseTime(final long query_timestamp) {
    // TODO - implement
    return 0;
  }
  
  public long rollupNextTime(final long query_timestamp) {
    // TODO - implement
    return 0;
  }
  
}

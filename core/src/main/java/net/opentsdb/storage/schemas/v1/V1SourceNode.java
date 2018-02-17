package net.opentsdb.storage.schemas.v1;

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;

public interface V1SourceNode extends TimeSeriesDataSource {

  public int parallelProcesses();
  
  // either the next chunk or end of query.
  public TimeStamp sequenceEnd();
}

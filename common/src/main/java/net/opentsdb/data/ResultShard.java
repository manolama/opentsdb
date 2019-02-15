package net.opentsdb.data;

import net.opentsdb.query.QueryNode;

/**
 * Instantiated once per time slice per node and used to keep track of how many
 * time series a node is emitting.
 *
 */
public interface ResultShard {

  // the computed number of shards we should expect from the data source
  // e.g. 6hr query for HBase would produce 6 shards.
  // Then if a downsample node was set to 2 hours, it would produce 3 shards (6 / 2)
  // 
  public int totalShards();
  
  // node this shard came from.
  public QueryNode node();
  
  // metric name
  public String dataSource();
  
  public TimeStamp start();
  
  public TimeStamp end();
  
  // lets the ResultSeries call into a shared map with a series ID has to get the
  // real series ID.
  // TODO - move this to the query pipeline context instead.
  public TimeSeriesId id(final long hash);
  
  // total number rof time series (ResultSeries) set at the end of the shard run
  // before calling complete(ResultShard) upstream so the upstream node knows when
  // we've fetched all of the data.
  public int timeSeriesCount();
  
  // wtf?
  public String sourceId();
  
  // TODO - how to handle the time spec.
  
  public void close();
}

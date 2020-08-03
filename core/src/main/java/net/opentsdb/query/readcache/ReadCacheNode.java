package net.opentsdb.query.readcache;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

public class ReadCacheNode implements TimeSeriesDataSource {

  @Override
  public QueryPipelineContext pipelineContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred initialize(Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QueryNodeConfig config() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence, long total_sequences) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(QueryResult next) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(PartialTimeSeries next) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onError(Throwable t) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void fetchNext(Span span) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String[] setIntervals() {
    // TODO Auto-generated method stub
    return null;
  }

}

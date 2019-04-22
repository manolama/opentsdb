package net.opentsdb.storage.file;

import com.stumbleupon.async.Deferred;

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.stats.Span;

public class FileNode  extends AbstractQueryNode implements TimeSeriesDataSource {
  FileFactory f;
  QueryPipelineContext pc;
  QueryNodeConfig c;
  
  public FileNode(final FileFactory f, final QueryPipelineContext pc, QueryNodeConfig c) {
    super(f, pc);
    this.f = f;
    this.pc = pc;
    this.c = c;
  }
  
  @Override
  public QueryPipelineContext pipelineContext() {
    return pc;
  }

  @Override
  public QueryNodeConfig config() {
    return c;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
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
    sendUpstream(new V3Result(this, f, f.c));
  }

}

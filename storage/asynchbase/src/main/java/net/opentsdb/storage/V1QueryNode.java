package net.opentsdb.storage;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.stumbleupon.async.Callback;

import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.meta.MetaDataPlugin;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.storage.V1Scanner.ScannerState;
import net.opentsdb.storage.schemas.v1.V1Result;
import net.opentsdb.storage.schemas.v1.V1SourceNode;

/**
 * 
 * This guy holds the state for a running query.
 * 
 * ASSUMPTIONS - 1.x TSD schema
 */
public class V1QueryNode extends AbstractQueryNode implements V1SourceNode {

  private AtomicInteger sequence_id = new AtomicInteger();
  private AtomicBoolean completed = new AtomicBoolean();
  private QuerySourceConfig config;
  private final net.opentsdb.stats.Span trace_span;
  private final AsyncHBaseDataStore store;
  private final V1Scanner[] scanners;
  
  // when the timestamp
  private TimeStamp sequence_end;
  // TODO - handle the case when we start scanning in chunks but we don't
  // have any data for the first few sequences. We should keep going at
  // that point.
  
  // TODO - UGGG handle fallback for rollups! If a series has a point 
  // where others don't, do we fallback?
  
  public V1QueryNode(final QueryNodeFactory factory,
                        final QueryPipelineContext context,
                        final QuerySourceConfig config,
                        final AsyncHBaseDataStore store) {
    super(factory, context);
    this.config = config;
    this.store = store;
    if (context.queryContext().stats() != null && 
        context.queryContext().stats().trace() != null) {
      trace_span = context.queryContext().stats().trace().firstSpan()
          .newChild("AsyncHBaseDataStore")
          .start();
    } else {
      trace_span = null;
    }
    
    // TODO - validate query
    // TODO - build the local processing pipeline
    
    // TODO - set {@code sequence_end}
    
    // TODO - See if we can use the same name for the meta plugin so it all
    // ties together.
    final MetaDataPlugin meta = (MetaDataPlugin) store.tsdb()
        .getRegistry().getPlugin(MetaDataPlugin.class, store.id());
    if (meta != null) {
      final List<byte[]> tsuids = meta.resolveTimeSeriesIds(config.query());
      if (tsuids != null) {
        // DO META!
      }
    }
    
    // if we're here no meta resolution so we go the slow way for tsd 1.x schema
    
    /**
     * workflow for filters:
     * 
     * - resolve tags. This storage plugin only cares about time, metrics 
     * and tags so far. Maybe values some day
     * - create scanner from time, metric and tags
     * - take filters out of the eval list
     * - on results
     *   - if no filters in eval list, we matched exactly!
     *   - but if there are filters, resolve TSUID and pass it through
     *   
     */
    
    // TODO - parse query
    // TODO - resolve tags
    
    // TODO - create HBase scanners/queries
    
    // TODO - real count
    int cnt = 20;
    scanners = new V1Scanner[cnt]; // TODO - real scanners
    for (int i = 0; i < cnt; i++) {
      scanners[i] = new V1Scanner(this, null /* REAL SCANNER */);
    }
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onComplete(final QueryNode downstream, 
                         final long final_sequence,
                         final long total_sequences) {
    throw new UnsupportedOperationException(store.id() + " is a data "
        + "source and cannot receive results.");
  }

  @Override
  public void onNext(final QueryResult next) {
    
    // TODO - if results are empty and we are segmented, then we should
    // call fetchNext();
    
    int seq = sequence_id.incrementAndGet();
    ((V1Result) next).setSequenceId(seq);
    for (final QueryNode node : upstream) {
      node.onNext(next);
    }
  }

  @Override
  public void onError(final Throwable t) {
    // TODO - something else?
    for (final QueryNode node : upstream) {
      node.onError(t);
    }
  }

  @Override
  public void fetchNext() {
    int completed = 0;
    for (final V1Scanner scanner : scanners) {
      if (scanner.state() == ScannerState.EXCEPTION) {
        // TODO - figure it out
      } else if (scanner.state() == ScannerState.COMPLETE) {
        completed++;
      }
    }
    
    if (completed == scanners.length) {
      final int seq = sequence_id.get();
      for (final QueryNode node : upstream) {
        node.onComplete(this, seq, seq);
      }
      return;
    }
    
    V1Result result = new V1Result(this);
    for (final V1Scanner scanner : scanners) {
      scanner.fetchNext(result);
    }
  }

  List<QueryFilter> scannerFilter() {
    // TODO
    return null;
  }
  
  UidSchema schema() {
    return null;
  }

  @Override
  public int parallelProcesses() {
    return scanners.length;
  }

  @Override
  public TimeStamp sequenceEnd() {
    return sequence_end;
  }
}

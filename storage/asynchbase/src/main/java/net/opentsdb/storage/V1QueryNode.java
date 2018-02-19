package net.opentsdb.storage;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.meta.MetaDataStorageSchema;
import net.opentsdb.meta.V1MetaResult;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryDownstreamException;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.storage.schemas.v1.V1Result;
import net.opentsdb.storage.schemas.v1.V1Schema;
import net.opentsdb.storage.schemas.v1.V1SourceNode;
import net.opentsdb.uid.ResolvedFilter;

/**
 * 
 * This guy holds the state for a running query.
 * 
 * for sharding where we may run out of space in a query result before
 * we've completed a full hour of data (or period) then there may be more
 * time series to group. In that case we let the grouping iterator call 
 * until it finds more data in time.
 * 
 * TODO - determine if we need to handle multiple metrics here too, or if 
 * we will ONLY have one metric per v1 query.
 * 
 * ASSUMPTIONS - 1.x TSD schema
 */
public class V1QueryNode extends AbstractQueryNode implements V1SourceNode, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(V1QueryNode.class);
  
  private AtomicInteger sequence_id = new AtomicInteger();
  private AtomicBoolean completed = new AtomicBoolean();
  private QuerySourceConfig config;
  private final net.opentsdb.stats.Span trace_span;
  private final UniqueIds uids;
  
  private V1Scanner[] scanners;
  private V1MultiGet multi_gets;
  private MetaDataStorageSchema meta;
  
  
  private Object initialized;
  
  // when the timestamp
  private TimeStamp sequence_end;
  // TODO - handle the case when we start scanning in chunks but we don't
  // have any data for the first few sequences. We should keep going at
  // that point.
  
  // TODO - UGGG handle fallback for rollups! If a series has a point 
  // where others don't, do we fallback?
  
  public V1QueryNode(final QueryNodeFactory factory,
                     final QueryPipelineContext context,
                     final QuerySourceConfig config) {
    super(factory, context);
    
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null!");
    }
    this.config = config;
    if (context.queryContext().stats() != null && 
        context.queryContext().stats().trace() != null) {
      trace_span = context.queryContext().stats().trace().firstSpan()
          .newChild("AsyncHBaseDataStore")
          .start();
    } else {
      trace_span = null;
    }
    
    uids = null;
    // TODO - may need some basic validation regarding the query.
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
    throw new UnsupportedOperationException(factory.id() + " is a data "
        + "source and cannot receive results.");
  }

  @Override
  public void onNext(final QueryResult next) {
    
    // TODO - if results are empty and we are segmented, then we should
    // call fetchNext();
    
    // TODO - based on the results determine if we need to fallback to higher
    // resolution data. In such cases we need to build an agg pipeline to 
    // perform the same steps that the pre-agg/rollup did.
    
    int seq = sequence_id.incrementAndGet();
    ((V1Result) next).setSequenceId(seq);
    for (final QueryNode node : upstream) {
      node.onNext(next);
    }
    
    int completed = 0;
    for (final V1Scanner scanner : scanners) {
      if (scanner.state() == StorageState.COMPLETE) {
        completed++;
      }
    }
    
    if (completed == scanners.length) {
      for (final QueryNode node : upstream) {
        node.onComplete(this, seq, seq);
      }
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
    // good ole double lock
    if (initialized == null) {
      synchronized(this) {
        if (initialized == null) {
          // TODO - thread pool it if needed.
          run();
        }
      }
    }
    
    if (multi_gets != null) {
      if (multi_gets.state() == StorageState.EXCEPTION) {
        // TODO - figure it out
        return;
      }
      
      if (multi_gets.state() == StorageState.COMPLETE) {
        final int seq = sequence_id.get();
        completeUpstream(seq, seq);
        return;
      }
      
      final V1Result result = new V1Result(this);
      multi_gets.fetchNext(result);
      return;
    }
    
    // not multi-gets so scan.
    int completed = 0;
    for (final V1Scanner scanner : scanners) {
      if (scanner.state() == StorageState.EXCEPTION) {
        // TODO - figure it out
      } else if (scanner.state() == StorageState.COMPLETE) {
        completed++;
      }
    }
    
    if (completed == scanners.length) {
      final int seq = sequence_id.get();
      completeUpstream(seq, seq);
      return;
    }
    
    final V1Result result = new V1Result(this);
    for (final V1Scanner scanner : scanners) {
      scanner.fetchNext(result);
    }
  }

  List<QueryFilter> scannerFilter() {
    // TODO
    return null;
  }
  
  V1Schema schema() {
    return ((V1AsyncHBaseDataStore) factory).schema();
  }

  @Override
  public int parallelProcesses() {
    return scanners.length;
  }

  @Override
  public TimeStamp sequenceEnd() {
    return sequence_end;
  }

  @Override
  public void run() {
    synchronized (this) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initializing query node: " + this);
      }
      initialized = new Object(); // tag it early but DON'T release
      // the lock until we're finished, otherwise other calls to
      // fetchNext() may progress before we're ready.
      
      // TODO - validate query
      
      // TODO - set {@code sequence_end}
      
      // TODO - set data type filters
      
      // eval meta first if configured
      // TODO - meta disable flag in config
      if (meta != null) {
        final V1MetaResult result = (V1MetaResult) meta.runQuery(config.query());
        switch(result.result()) {
        case DATA:
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received results from meta store, setting up "
                + "multi-gets.");
          }
          multi_gets = new V1MultiGet(this, result);
          return;
        case NO_DATA:
          if (LOG.isDebugEnabled()) {
            LOG.debug("No data returned from meta store: " + meta);
          }
          completeUpstream(0, 0);
          return;
        case EXCEPTION:
          LOG.warn("Unrecoverable exception from meta store: " + meta, 
              result.exception());
          sendUpstream(result.exception());
          return;
        case NO_DATA_FALLBACK:
          if (LOG.isDebugEnabled()) {
            LOG.debug("No data returned from meta store: " + meta 
                + ". Falling back to scans.");
          }
          break; // fall through to scans
        case EXCEPTION_FALLBACK:
          LOG.warn("Exception from meta store, falling back: " 
              + meta, result.exception());
          break;
        default: // fall through to scans
          final QueryDownstreamException ex = new QueryDownstreamException(
              "Unhandled meta result type: " + result.result());
          LOG.error("WTF? Shouldn't happen.", ex);
          sendUpstream(ex);
          return;
        }
      }
      
      // if we're here no meta resolution so we go the slow way for tsd 1.x schema
      setupScanners();
    }
  }
  
  void setupScanners() {

    /**
     * workflow for filters:
     * 
     * - resolve tags. This storage plugin only cares about time, metrics 
     * and tags so far. Maybe values some day
     * - create scanner from time, metric and tags
     * - take filters out of the eval list
     * - on results
     *   - if no filters in eval list, we matched exactly!
     *   - but if there are filters, resolve TSUID to ID (or do we have the filter know the schema?) and pass it through
     *   
     */
    
    // TODO - proper query when we're there, for now, old school
    net.opentsdb.query.pojo.TimeSeriesQuery query = 
        (net.opentsdb.query.pojo.TimeSeriesQuery) config.query();
    
    class ResolveCB implements Callback<Deferred<Object>, List<ResolvedFilter>> {
      
      final Metric metric;
      final Filter filter;
      
      ResolveCB(final Metric metric, final Filter filter) {
        this.metric = metric;
        this.filter = filter;
      }
      
      @Override
      public Deferred<Object> call(final List<ResolvedFilter> resolved) throws Exception {
        // TODO Auto-generated method stub
        return Deferred.fromResult(null);
      }
      
    }
    
    final List<Deferred<Object>> deferreds = Lists.newArrayList();
    for (final Metric metric : query.getMetrics()) {
      if (!Strings.isNullOrEmpty(metric.getFilter())) {
        final Filter filter = query.getFilter(metric.getFilter());
        deferreds.add(uids.resolveUids(filter).addCallbackDeferring(new ResolveCB(metric, filter)));
        continue;
      }
      
      // just a metric, setup simple scanners.
    }
    
    // TODO - create HBase scanners/queries
    
    // TODO - real count
    int cnt = 20;
    scanners = new V1Scanner[cnt]; // TODO - real scanners
    for (int i = 0; i < cnt; i++) {
      scanners[i] = new V1Scanner(this, null /* REAL SCANNER */);
    }
  }

  HBaseClient client() {
    return ((V1AsyncHBaseDataStore) factory).client();
  }
}

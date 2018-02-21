package net.opentsdb.query.uidresolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;

public class ByteIdResolver extends AbstractQueryNode {

  public ByteIdResolver(QueryNodeFactory factory, QueryPipelineContext context) {
    super(factory, context);
  }

  @Override
  public QueryNodeConfig config() {
    // TODO Auto-generated method stub
    return null;
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
  public void onComplete(QueryNode downstream, long final_sequence,
      long total_sequences) {
    completeUpstream(final_sequence, total_sequences);
  }

  @Override
  public void onNext(QueryResult next) {
    new ResolvedResult(next);
  }

  @Override
  public void onError(Throwable t) {
    sendUpstream(t);
  }

  class ResolvedResult implements QueryResult {
    /** The non-null query results. */
    private final QueryResult results;
    
    private List<TimeSeries> new_series;
    private List<Deferred<Object>> deferreds;
    
    class IDCB implements Callback<Object, TimeSeriesStringId> {
      final TimeSeries series;
      IDCB(final TimeSeries series) {
        this.series = series;
      }
      @Override
      public Object call(final TimeSeriesStringId id) throws Exception {
        synchronized(new_series) {
          new_series.add(new TSWrapper(id, series));
        }
        System.out.println("Resolved an id: " + id);
        return null;
      }
      
    }
    
    class FinalCB implements Callback<Object, ArrayList<Object>> {

      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        System.out.println("RESOLVED! Sending upstream.");
        sendUpstream(ResolvedResult.this);
        return null;
      }
      
    }
    
    private ResolvedResult(final QueryResult results) {
      this.results = results;
      new_series = Lists.newArrayListWithCapacity(results.timeSeries().size());
      deferreds = Lists.newArrayListWithCapacity(results.timeSeries().size());
      
      for (final TimeSeries series : results.timeSeries()) {
        System.out.println("RESOLVING ID: " + series.id());
        deferreds.add(series.id().decode().addCallback(new IDCB(series)));
      }
      
      Deferred.group(deferreds).addCallback(new FinalCB())
      .addErrback(new Callback<Object, Exception>() {

        @Override
        public Object call(Exception arg0) throws Exception {
          arg0.printStackTrace();
          return null;
        }
        
      });
    }
    
    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeries> timeSeries() {
      return new_series;
    }

    @Override
    public long sequenceId() {
      return results.sequenceId();
    }

    @Override
    public QueryNode source() {
      return ByteIdResolver.this;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return TypeToken.of(TimeSeriesStringId.class);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  class TSWrapper implements TimeSeries {
    final TimeSeriesId new_id;
    final TimeSeries source;
    
    TSWrapper(final TimeSeriesId new_id, final TimeSeries source) {
      this.new_id = new_id;
      this.source = source;
    }

    @Override
    public TimeSeriesId id() {
      return new_id;
    }

    @Override
    public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
        TypeToken<?> type) {
      return source.iterator(type);
    }

    @Override
    public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
      return source.iterators();
    }

    @Override
    public Collection<TypeToken<?>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }
    
  }
}

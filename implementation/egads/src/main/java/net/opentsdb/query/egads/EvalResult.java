package net.opentsdb.query.egads;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.swing.text.html.HTMLDocument.Iterator;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.RollupConfig;

public class EvalResult implements QueryResult {

  final QueryNode node;
  final QueryResult original_result;
  final List<TimeSeries> series;
  
  public EvalResult(final QueryNode node, final QueryResult original_result) {
    this.node = node;
    this.original_result = original_result;
    series = Lists.newArrayList(original_result.timeSeries());
  }
  
  public void addPredictionsAndThresholds(final QueryResult result) {
    for (final TimeSeries ts : result.timeSeries()) {
      if (ts.types().contains(NumericArrayType.TYPE)) {
        series.add(new AlignedArrayTimeSeries(ts, result));
      } else {
        series.add(ts);
      }
    }
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return original_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public String dataSource() {
    return original_result.dataSource();
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return original_result.idType();
  }

  @Override
  public ChronoUnit resolution() {
    return original_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return original_result.rollupConfig();
  }

  @Override
  public void close() {
    original_result.close();
  }

  @Override
  public boolean processInParallel() {
    return true;
  }
  
  class AlignedArrayTimeSeries implements TimeSeries {
    final TimeSeries source;
    final QueryResult result;
    
    AlignedArrayTimeSeries(final TimeSeries source, final QueryResult result) {
      this.source = source;
      this.result = result;
    }
    
    @Override
    public TimeSeriesId id() {
      return source.id();
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (type != NumericArrayType.TYPE) {
        return source.iterator(type);
      }
      return Optional.of(new ArrayIterator());
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its =
          Lists.newArrayListWithExpectedSize(1);
      for (final TypeToken<? extends TimeSeriesDataType> type : source.types()) {
        if (type == NumericArrayType.TYPE) {          
          its.add(new ArrayIterator());
        } else {
          its.add(source.iterator(type).get());
        }
      }
      return its;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }
    
    class ArrayIterator implements TypedTimeSeriesIterator<NumericArrayType>, 
      TimeSeriesValue<NumericArrayType>, NumericArrayType {
      
      final TimeSeriesValue<NumericArrayType> value;
      final int start_idx;
      final int end_idx;
      boolean has_next;
      
      ArrayIterator() {
        final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op =
            source.iterator(NumericArrayType.TYPE);
        if (!op.isPresent()) {
          value = null;
          start_idx = end_idx = -1;
          return;
        }
        
        final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator =
            op.get();
        if (!iterator.hasNext()) {
          value = null;
          start_idx = end_idx = -1;
          return;
        }
        
        value = (TimeSeriesValue<NumericArrayType>) iterator.next();
        TimeStamp ts = result.timeSpecification().start().getCopy();
        int i = value.value().offset();
        int x = i;
        while (ts.compare(Op.LT, original_result.timeSpecification().start())) {
          i++;
          x++;
          ts.add(original_result.timeSpecification().interval());
        }
        
        while (ts.compare(Op.LT, original_result.timeSpecification().end())) {
          x++;
          ts.add(original_result.timeSpecification().interval());
        }
        
        start_idx = i;
        end_idx = x;
        has_next = end_idx > start_idx;
      }
      
      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericArrayType> next() {
        has_next = false;
        return this;
      }

      @Override
      public TimeStamp timestamp() {
        return value.timestamp();
      }

      @Override
      public NumericArrayType value() {
        return this;
      }

      @Override
      public TypeToken<NumericArrayType> type() {
        return NumericArrayType.TYPE;
      }

      @Override
      public TypeToken<NumericArrayType> getType() {
        return NumericArrayType.TYPE;
      }

      @Override
      public int offset() {
        return start_idx;
      }

      @Override
      public int end() {
        return end_idx;
      }

      @Override
      public boolean isInteger() {
        return value.value().isInteger();
      }

      @Override
      public long[] longArray() {
        return value.value().longArray();
      }

      @Override
      public double[] doubleArray() {
        return value.value().doubleArray();
      }
      
    }
  }
}

package net.opentsdb.query.processor.bucketpercentile;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.data.BaseTimeSeriesList;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.XXHash;

/**
 * Indexing is fun here in order to avoid creating tons and tons of arrays.
 * 
 * The final list has an array of the series for each tag set.
 * When we iterate or fetch from the "List" we consider the number of percentiles
 * requested so that the "size" of the time series collection is the number of 
 * percentiles times the cardinality. When we get an index into the list (or 
 * iterate) then we consider it as:
 * real1                real2
 * p1_1   p2_1   p3_1   p2_1   p2_2   p2_3
 * 0      1      2      3      4      5
 */
public class BucketPercentileResult extends BaseTimeSeriesList implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(BucketPercentileResult.class);
  
  private final BucketPercentile node;
  private TLongObjectMap<TimeSeries[]> map;
  private QueryResult initial_result;
  private TimeSeries[][] final_series;
  private int last_get = -1;
  private final List<Double> percentiles;
  Computer last_ts;
  
  BucketPercentileResult(final BucketPercentile node) {
    this.node = node;
    percentiles = ((BucketPercentileConfig) node.config()).getPercentiles();
    map = new TLongObjectHashMap<TimeSeries[]>();
  }
  
  void addResult(final QueryResult result) {
    System.out.println("***************** RESULT!: " + result.dataSource());
    // TODO - need one with results ideally.
    if (initial_result == null) {
      initial_result = result;
    }
    
    if (result.timeSeries() == null || result.timeSeries().size() < 1) {
      return;
    }
    
    int bucket_index = -1;
    for (int i = 0; i < result.timeSeries().size(); i++) {
      final TimeSeries series = result.timeSeries().get(i);
      System.out.println("************ WORKING series: " + series.id());
      final long hash = hashStringId((TimeSeriesStringId) series.id());
      
      if (i == 0) {
        // have to find the bucket index first.
        // TODO - String vs Byte Ids. Means we need to resolve byte ids.. sniff
        TimeSeriesStringId id = (TimeSeriesStringId) series.id();
        for (int x = 0; x < node.buckets().length; x++) {
          if (node.buckets()[x].metric.equals(id.metric())) {
            bucket_index = x;
            break;
          }
        }
        if (bucket_index < 0) {
          LOG.error("??? The result set with metric: " + id.metric() 
            + " didn't match a bucket?");
          return;
        }
      }
      
      TimeSeries[] ts = map.get(hash);
      if (ts == null) {
        ts = new TimeSeries[node.buckets().length];
        map.put(hash, ts);
      }
      ts[bucket_index] = series;
    }
  }
  
  void finishSetup() {
    final_series = new TimeSeries[map.size()][];
    final TLongObjectIterator<TimeSeries[]> iterator = map.iterator();
    int i = 0; 
    while (iterator.hasNext()) {
      iterator.advance();
      final_series[i++] = iterator.value();
    }
    size = final_series.length * percentiles.size();
    System.out.println("************ SIZE: " + size);
    map = null;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return initial_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return this;
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return (QueryResultId) ((BucketPercentileConfig) node.config()).resultIds().get(0);
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ChronoUnit resolution() {
    return initial_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return initial_result.rollupConfig();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean processInParallel() {
    // TODO - we may need the listerator and thread locals?
    return false;
  }

  @Override
  public Iterator<TimeSeries> iterator() {
    // TODO Auto-generated method stub
    return new LocalIterator();
  }

  class LocalIterator implements Iterator<TimeSeries> {
    int idx = 0;
    
    @Override
    public boolean hasNext() {
      return idx < size;
    }

    @Override
    public TimeSeries next() {
      return get(idx++);
    }
    
  }
  
  @Override
  public TimeSeries get(final int index) {
    int real_idx = index / percentiles.size();
    if (last_ts != null && last_ts.index == real_idx) {
      // cool, re-use!
    } else {
      final TimeSeries[] set = final_series[real_idx];
      // TODO - Yes we assume they're all the same. They better be. Add a check.
      TypeToken<?> type = null;
      for (int i = 0; i < set.length; i++) {
        if (set[i] != null) {
          // TODO - maybe could be more than one... grr
          type = set[i].types().iterator().next();
          break;
        }
      }

      if (last_ts != null) {
        try {
          last_ts.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      
      if (type == NumericArrayType.TYPE) {
        last_ts = new BucketPercentileNumericArrayComputation(real_idx, node, set);
      } else {
        // TODO - the others
        throw new IllegalStateException("BAD TYPE: " + type);
      }
      
      last_ts.run();
    }
    
    int ptile = index - (real_idx * percentiles.size());
    return last_ts.getSeries(ptile);
  }
  
  long hashStringId(final TimeSeriesStringId id) {
    // super critically important that we sort the tags.
    final Map<String, String> sorted_tags;
    if (id.tags() == null) {
      sorted_tags = Collections.emptyMap();
    } else if (id.tags() instanceof NavigableMap) {
      sorted_tags = id.tags();
    } else if (!id.tags().isEmpty()) {
      sorted_tags = new TreeMap<String, String>(id.tags());
    } else {
      sorted_tags = Collections.emptyMap();
    }
    
    long hash = 0;
    if (sorted_tags != null) {
      for (final Entry<String, String> entry : sorted_tags.entrySet()) {
        if (hash == 0) {
          hash = XXHash.hash(entry.getKey());
        } else {
          hash = XXHash.updateHash(hash, entry.getKey());
        }
        hash = XXHash.updateHash(hash, entry.getValue());
      }
    }
    return hash;
  }
}
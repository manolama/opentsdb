package net.opentsdb.query.processor.bucketpercentile;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;

public class BucketPercentile extends AbstractQueryNode {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketPercentile.class);
  
  private final BucketPercentileConfig config;
  private final Map<QueryResultId, QueryResult> results;
  private final Bucket[] buckets;
  
  public BucketPercentile(final QueryNodeFactory factory, 
                          final QueryPipelineContext context,
                          final BucketPercentileConfig config) {
    super(factory, context);
    this.config = config;
    results = Maps.newConcurrentMap();
    if (config.underFlowId() != null) {
      results.put(config.underFlowId(), DUMMY);
    }
    if (config.overFlowId() != null) {
      results.put(config.overFlowId(), DUMMY);
    }
    
    for (final QueryResultId id : config.histogramIds()) {
      results.put(id, DUMMY);
    }
    
    // parse out the metrics
    LOG.info("***** RESULTS SIZE: " + results.size());
    buckets = new Bucket[results.size()];
    int bucket_idx = 0;
    for (final String histogram : config.histogramMetrics()) {
      final Matcher matcher = config.pattern().matcher(histogram);
      if (!matcher.find()) {
        throw new IllegalArgumentException("Histogram metric [" + histogram 
            + "] did not match the regex pattern: " + config.getBucketRegex());
      }
      if (matcher.groupCount() < 2) {
        throw new IllegalArgumentException("Histogram metric [" + histogram 
            + "] did not match two groups from the regex pattern: " 
            + config.getBucketRegex());
      }
      double lower = Double.NaN;
      try {
         lower = Double.parseDouble(matcher.group(1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse lower bucket from "
            + "metric [" + histogram + "] using pattern: " 
            + config.getBucketRegex() + " that matched: " + matcher.group(1));
      }
      
      double upper = Double.NaN;
      try {
        upper = Double.parseDouble(matcher.group(2));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Failed to parse upper bucket from "
            + "metric [" + histogram + "] using pattern: " 
            + config.getBucketRegex() + " that matched: " + matcher.group(2));
      }
      
      LOG.info("          LW: " + lower + "   UP: " + upper);
      buckets[bucket_idx++] = new Bucket(histogram, lower, upper);
    }
    
    if (!Strings.isNullOrEmpty(config.underFlowMetric())) {
      buckets[bucket_idx++] = new Bucket(config.underFlowMetric(), false, true);
    }
    
    if (!Strings.isNullOrEmpty(config.overFlowMetric())) {
      buckets[bucket_idx++] = new Bucket(config.overFlowMetric(), true, false);
    }
    
    Arrays.sort(buckets, BUCKET_COMPARATOR);
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onNext(final QueryResult next) {
    LOG.info("GOT RESULT: " + next.dataSource());
    if (results.get(next.dataSource()) != DUMMY) {
      LOG.error("Unexpected result: " + next.dataSource());
      return;
    }
    
    results.put(next.dataSource(), next);
    int dummies = 0;
    for (final Entry<QueryResultId, QueryResult> entry : results.entrySet()) {
      if (entry.getValue() == DUMMY) {
        dummies++;
      }
    }
    
    if (dummies == 0) {
      LOG.debug("All results in! Processing.");
    } else {
      LOG.debug("Still waiting: Dummies: " + dummies);
      return;
    }
    
    BucketPercentileResult result = new BucketPercentileResult(this);
    for (final Entry<QueryResultId, QueryResult> entry : results.entrySet()) {
      result.addResult(entry.getValue());
    }
    result.finishSetup();
    sendUpstream(result);
  }
  
  Bucket[] buckets() {
    return buckets;
  }
  
  static class DummyResult implements QueryResult {

    @Override
    public TimeSpecification timeSpecification() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<TimeSeries> timeSeries() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String error() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public long sequenceId() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public QueryNode source() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryResultId dataSource() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ChronoUnit resolution() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RollupConfig rollupConfig() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public boolean processInParallel() {
      // TODO Auto-generated method stub
      return false;
    }
    
  }
  private final static DummyResult DUMMY = new DummyResult();

  class Bucket {
    String metric;
    double lower;
    double upper;
    double report;
    boolean is_overflow;
    boolean is_underflow;
    
    Bucket(String metric, double lower, double upper) {
      this.metric = metric;
      this.lower = lower;
      this.upper = upper;
      switch (config.getOutputOfBucket()) {
      case BOTTOM:
        report = lower;
        LOG.info("******* lower: " + metric + "  REPORT: " + report);
        break;
      case TOP:
        report = upper;
        LOG.info("******* upper: " + metric + "  REPORT: " + report);
        break;
      case MEAN:
        report = lower + (upper - lower) / 2;
        LOG.info("******* mean: " + metric + "  REPORT: " + report);
        break;
      default:
        throw new IllegalArgumentException("No handler for: " + config.getOutputOfBucket());
      }
    }
    
    Bucket(String metric, boolean is_overflow, boolean is_underflow) {
      this.metric = metric;
      this.is_overflow = is_overflow;
      this.is_underflow = is_underflow;
      if (is_overflow) {
        report = config.getOverFlowMax();
      } else {
        report = config.getUnderFlowMin();
      }
    }
    
  }
  
  static class BucketComparator implements Comparator<Bucket> {

    @Override
    public int compare(final Bucket o1, final Bucket o2) {
      if (o1.is_underflow) {
        return -1;
      } else if (o1.is_overflow) {
        return 1;
      }
      return o1.lower < o2.lower ? -1 : 
          o1.lower == o2.lower ? 0 : 1;
    }
    
  }
  private static final BucketComparator BUCKET_COMPARATOR = new BucketComparator();
}

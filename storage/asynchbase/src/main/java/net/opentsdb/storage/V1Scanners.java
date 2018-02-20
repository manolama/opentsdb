package net.opentsdb.storage;

import java.util.List;

import org.hbase.async.Bytes;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.schemas.v1.V1Result;
import net.opentsdb.storage.schemas.v1.V1RollupInterval;
import net.opentsdb.storage.schemas.v1.V1Schema;
import net.opentsdb.uid.ResolvedFilter;

public class V1Scanners {
  private static final Logger LOG = LoggerFactory.getLogger(V1Scanners.class);
  
  private final net.opentsdb.query.pojo.TimeSeriesQuery query;
  private final V1QueryNode node;
  private final Metric metric;
  private final Filter filter;
  private final List<ResolvedFilter> resolved_uids;
  private final byte[] metric_uid;
  private V1Scanner[] scanners;
  private List<RollupInterval> rollup_intervals;
  private int rollup_interval_index = 0;
  private List<TagVFilter> remaining_filters;
  
  V1Scanners(final V1QueryNode node, net.opentsdb.query.pojo.TimeSeriesQuery query, final Metric metric, final Filter filter, List<ResolvedFilter> resolved_uids, final byte[] metric_uid) {
    this.query = query;
    this.node = node;
    this.metric = metric;
    this.filter = filter;
    this.resolved_uids = resolved_uids;
    this.metric_uid = metric_uid;
    
    scanners = new V1Scanner[node.schema().saltWidth() > 0 ?
                             node.schema().saltBuckets() + 1 : 1];
    
    if (node.schema().rollupConfig() != null) {
      if (metric.getDownsampler() != null) {
        rollup_intervals = node.schema().rollupConfig().getRollupIntervals(metric.getDownsampler().getInterval());
      } else if (query.getTime().getDownsampler() != null) {
        rollup_intervals = node.schema().rollupConfig().getRollupIntervals(query.getTime().getDownsampler().getInterval());
      }
    }
    
    final ScanFilter scan_filter = buildFilter();
    
    final byte[] start_key = setStartKey();
    final byte[] end_key = setEndKey();
    
    for (int i = 0; i < scanners.length; i++) {
      final Scanner scanner = newScanner();
      
      if (scanners.length > 1)  {
        byte[] key = new byte[start_key.length];
        System.arraycopy(start_key, 0, key, 0, start_key.length);
        node.schema().setSaltBucket(i, key);
        scanner.setStartKey(key);
        
        key = new byte[end_key.length];
        System.arraycopy(end_key, 0, key, 0, end_key.length);
        node.schema().setSaltBucket(i, key);
        scanner.setStopKey(key);
      } else {
        scanner.setStartKey(start_key);
        scanner.setStopKey(end_key);
      }
      
      if (scan_filter != null) {
        scanner.setFilter(scan_filter);
      }
      
      scanners[i] = new V1Scanner(node, scanner);
    }
    
    // READY!
  }
  
  byte[] setStartKey() {
    long start_ts = query.getTime().startTime().epoch();
    
    if (rollup_intervals != null) {
      start_ts = ((V1RollupInterval) rollup_intervals.get(rollup_interval_index)).rollupBaseTime(start_ts);
      // TODO - if rate, roll back one interval
    } else {
      if (metric.getDownsampler() != null) {
        start_ts = node.schema().alignDownsamplerBaseTimestamp(metric.getDownsampler(), start_ts);
      } else if (query.getTime().getDownsampler() != null) {
        start_ts = node.schema().alignDownsamplerBaseTimestamp(query.getTime().getDownsampler(), start_ts);
      } else {
        // neither rollup nor downsample so hit the raw or pre-agg table
        start_ts = node.schema().alignQueryBaseTimestamp(start_ts);
      }
    }
    
    // Don't return negative numbers.
    start_ts = start_ts > 0L ? start_ts : 0L;
    
    final byte[] start_key = new byte[node.schema().saltWidth() + 
                                      node.schema().metricUIDBytes() +
                                      V1Schema.TIMESTAMP_BYTES];
    System.arraycopy(metric_uid, 0, start_key, node.schema().saltWidth(), metric_uid.length);
    Bytes.setInt(start_key, (int) start_ts, (node.schema().saltWidth() + 
                                             node.schema().metricUIDBytes()));
    return start_key;
  }
  
  byte[] setEndKey() {
    long end_ts = query.getTime().endTime().epoch();
    
    if (rollup_intervals != null) {
      end_ts = ((V1RollupInterval) rollup_intervals.get(rollup_interval_index)).rollupNextTime(end_ts);
      // TODO - if rate, roll back one interval
    } else {
      if (metric.getDownsampler() != null) {
        end_ts = node.schema().alignDownsamplerNextTimestamp(metric.getDownsampler(), end_ts);
      } else if (query.getTime().getDownsampler() != null) {
        end_ts = node.schema().alignDownsamplerNextTimestamp(query.getTime().getDownsampler(), end_ts);
      } else {
        // neither rollup nor downsample so hit the raw or pre-agg table
        end_ts = node.schema().alignQueryNextTimestamp(end_ts);
      }
    }
    
    // Don't return negative numbers.
    end_ts = end_ts > 0L ? end_ts : 0L;
    final byte[] end_key = new byte[node.schema().saltWidth() + 
                                      node.schema().metricUIDBytes() +
                                      V1Schema.TIMESTAMP_BYTES];
    System.arraycopy(metric_uid, 0, end_key, node.schema().saltWidth(), metric_uid.length);
    Bytes.setInt(end_key, (int) end_ts, (node.schema().saltWidth() + 
                                             node.schema().metricUIDBytes()));
    return end_key;
  }
  
  void fetchNext(final V1Result result) {
    for (final V1Scanner scanner : scanners) {
      scanner.fetchNext(result);
    }
  }
  
  ScanFilter buildFilter() {
    // TODO - build it including rollups!
    return null;
  }
  
  String getRowKeyUIDRegex() {
    // TODO - build me
    return null;
  }

  StorageState state() {
    // TODO - implement
    return null;
  }
  
  private Scanner newScanner() {
    final Scanner scanner;
    // we first need to pick the table if rollups are enabled.
    if (node.schema().rollupConfig() == null) {
      // TODO - determine if pre-agg query
      scanner = node.client().newScanner(((V1AsyncHBaseDataStore) node.factory()).dataTable());
      scanner.setFamily(((V1AsyncHBaseDataStore) node.factory()).columnFamily());
    } else {
      // it's a rollup
      // TODO - do it
      scanner = null;
    }
    
    return scanner;
  }
}

package net.opentsdb.storage;

import java.util.List;

import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.storage.schemas.v1.V1Result;
import net.opentsdb.uid.ResolvedFilter;

public class V1Scanners {
  private static final Logger LOG = LoggerFactory.getLogger(V1Scanners.class);
  
  private final net.opentsdb.query.pojo.TimeSeriesQuery query;
  private final V1QueryNode node;
  private final Metric metric;
  private final Filter filter;
  private final List<ResolvedFilter> resolved_uids;
  private V1Scanner[] scanners;
  
  V1Scanners(final V1QueryNode node, net.opentsdb.query.pojo.TimeSeriesQuery query, final Metric metric, final Filter filter, List<ResolvedFilter> resolved_uids) {
    this.query = query;
    this.node = node;
    this.metric = metric;
    this.filter = filter;
    this.resolved_uids = resolved_uids;
    
    scanners = new V1Scanner[node.schema().saltBuckets() + 1];
    
    final byte[] start_key = null; // TODO - get me
    final byte[] stop_key = null; // TODO - ge tme
    
    final ScanFilter scan_filter = buildFilter();
    
    for (int i = 0; i < scanners.length; i++) {
      final Scanner scanner = node.client().newScanner(new byte[] { } /* TODO - fix me! */);
      //scanner.setFamilies(arg0); // TODO - set
      
      // TODO - copy keys and set salt if necessary
      scanner.setStartKey(start_key);
      scanner.setStopKey(stop_key);
      if (scan_filter != null) {
        scanner.setFilter(scan_filter);
      }
      
      scanners[i] = new V1Scanner(node, scanner);
    }
    
    // READY!
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
}

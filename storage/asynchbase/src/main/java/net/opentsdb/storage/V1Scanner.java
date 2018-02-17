package net.opentsdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;

import net.opentsdb.data.TimeSeriesQueryId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.RelationalOperator;
import net.opentsdb.storage.schemas.v1.V1Result;
import net.opentsdb.utils.ConcurrentByteMap;

/**
 * One scanner. May be a salt scanner, may be the only one.
 */
public class V1Scanner {
  private static final Logger LOG = LoggerFactory.getLogger(V1Scanner.class);
  
  TimeSeriesQueryId sentinel = null; // TODO - fix me!
  
  enum ScannerState {
    COMPLETE,
    EXCEPTION,
    CONTINUE
  }
  
  private final V1QueryNode node;
  private final Scanner scanner;
  private ScannerState state;
  
  private Set<Byte> data_type_filter;
  
  // TODO - make this an LRU
  private ConcurrentByteMap<TimeSeriesQueryId> keys_to_ids;
  
  private List<ArrayList<KeyValue>> row_buffer;
    
  V1Scanner(final V1QueryNode node, final Scanner scanner) {
    this.node = node;
    this.scanner = scanner;
    state = ScannerState.CONTINUE;
  }
  
  public void fetchNext(final V1Result result) {
    if (result.hasException()) {
      scanner.close();
      // TODO - log
      return;
    }
    
    if (result.isFull()) {
      // TODO - log
      result.scannerDone();
      return;
    }
    
    if (row_buffer != null) {
      // copy so we can delete and create a new one if necessary
      final List<ArrayList<KeyValue>> row_buffer = this.row_buffer;
      this.row_buffer = null;
      for (final ArrayList<KeyValue> row : row_buffer) {
        decode(row, result);
      }
    }
    
    if (result.isFull()) {
      // still continuing
      result.scannerDone();
      return;
    }
    
    // try for some more!
    scanner.nextRows().addCallbacks(new ScannerCB(result), new ErrorCB(result));
  }
  
  final class ScannerCB implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    private final V1Result result;
    
    ScannerCB(final V1Result result) {
      this.result = result;
    }

    @Override
    public Object call(final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
      if (result.hasException()) {
        // bail out!
        complete();
        return null;
      }
      
      if (rows == null) {
        complete();
        return null;
      }
      
      // TODO - also stop at the next hour or boundary
      
      if (node.scannerFilter() != null) {
        for (final ArrayList<KeyValue> row : rows) {
          if (row.isEmpty()) {
            // should never happen
            if (LOG.isDebugEnabled()) {
              LOG.debug("Received an empty row from result set: " + rows);
            }
            continue;
          }
          
          final byte[] tsuid = node.schema().timelessKey(row.get(0).key());
          TimeSeriesQueryId id = keys_to_ids.get(tsuid);
          if (id == null) {
            if (keys_to_ids.putIfAbsent(tsuid, sentinel) == null) {
              // start resolution of the tags to strings, then filter
            } else {
              // TODO - find out how to glom on to the sentinel for resolution.
            }
          }
        }
      } else {
        // load all
        for (final ArrayList<KeyValue> row : rows) {
          if (row.isEmpty()) {
            // should never happen
            if (LOG.isDebugEnabled()) {
              LOG.debug("Received an empty row from result set: " + rows);
            }
            continue;
          }
          decode(row, null);
        }
      }
      
      if (!result.isFull()) {
        // keep going!
        scanner.nextRows().addCallbacks(this, new ErrorCB(result));
      }
      if (result.hasException()) {
        complete();
      } else {
        result.scannerDone();
      }
      return null;
    }
    
    void complete() {
      if (!result.hasException()) {
        state = ScannerState.COMPLETE;
      }
      state = ScannerState.EXCEPTION;
      result.scannerDone();
      scanner.close(); // TODO - attach a callback for logging in case
      // something goes pear shaped.
    }
  }
  
  ScannerState state() {
    return state;
  }
  
  void close() {
    scanner.close();
  }
  
  void decode(final ArrayList<KeyValue> row, final V1Result result) {
    // ASUME we have checked for a non-null and non-empty row
    // we have to pre-process here to remove duplicates and find out
    // the types of data we have.
    
    final TimeStamp base = node.schema().baseTimestamp(row.get(0).key());
    
    if (result.isFull() || base.compare(RelationalOperator.GTE, node.sequenceEnd())) {
      // store the rest in the buffer
      if (row_buffer == null) {
        row_buffer = Lists.newArrayList();
      }
      row_buffer.add(row);
      return;
    }
    
    final byte[] tsuid = node.schema().timelessKey(row.get(0).key());
    for (final KeyValue kv : row) {
      if ((kv.qualifier().length & 1) == 0) {
        // it's a NumericDataType
        if (data_type_filter != null && !data_type_filter.contains((byte) 1)) {
          // filter doesn't want #'s
          // TODO - dropped counters
          continue;
        }
        result.addData(base, tsuid, (byte) 0, kv.qualifier(), kv.value());
      } else {
        final byte prefix = kv.qualifier()[0];
        if (prefix == 5 /* appends, don't hard code */) {
          if (!data_type_filter.contains((byte) 1)) {
            // filter doesn't want #'s
            continue;
          } else {
            result.addData(base, tsuid, prefix, kv.qualifier(), kv.value());
          }
        } else if (data_type_filter.contains(prefix)) {
          result.addData(base, tsuid, prefix, kv.qualifier(), kv.value());
        }
        // TODO else count dropped data
      }
    }
  }
  
  final class ErrorCB implements Callback<Object, Exception> {
    private final V1Result result;
    
    ErrorCB(final V1Result result) {
      this.result = result;
    }
    @Override
    public Object call(final Exception ex) throws Exception {
      state = ScannerState.EXCEPTION;
      result.scannerDone();
      return null;
    }
  }
}

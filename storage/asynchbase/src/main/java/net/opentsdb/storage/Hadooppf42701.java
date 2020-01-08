package net.opentsdb.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.rollup.DefaultRollupConfig;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.schemas.tsdb1x.NumericSummaryRowSeq;
import net.opentsdb.storage.schemas.tsdb1x.NumericSummarySpan;
import net.opentsdb.storage.schemas.tsdb1x.RowSeq;
import net.opentsdb.storage.schemas.tsdb1x.SchemaFactory;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

public class Hadooppf42701 {

  public static void main(final String[] args) {
    
    final Configuration config = new Configuration(args);
    DefaultTSDB tsdb = new DefaultTSDB(config);
    try {
      System.out.println("Initializing TSDB...");
      tsdb.initializeRegistry(true).join(300000);
      System.out.println("Initialized TSDB!");
      
      SchemaFactory sf = (SchemaFactory) tsdb.getRegistry().getPlugin(TimeSeriesDataSourceFactory.class, "BF");
      Tsdb1xHBaseDataStore store = (Tsdb1xHBaseDataStore) sf.schema.data_store;
      System.out.println(" GOT THE STORE: " + store);
      
      System.out.println("Opening scanners...");
      Scanner original_scanner = store.client.newScanner("yamas:rollup-1h-noconvert");
      Scanner new_scanner = store.client.newScanner("yamas:rollup-1h-convert");
      new_scanner.setMaxNumBytes(Long.MAX_VALUE);
      System.out.println("Got scanners.");
      
      RollupConfig rc = DefaultRollupConfig.newBuilder()
          .addAggregationId("sum", 0)
          .addAggregationId("count", 1)
          .addAggregationId("min", 2)
          .addAggregationId("max", 3)
          .addInterval(RollupInterval.builder()
            .setInterval("1h")
            .setPreAggregationTable("foo")
            .setRowSpan("1d")
            .setTable("foo"))
          .build();
      RollupInterval interval = ((DefaultRollupConfig) rc).getRollupInterval("1h");
      
      
      ArrayList<ArrayList<KeyValue>> original_rows = original_scanner.nextRows().join();
      ArrayList<ArrayList<KeyValue>> new_rows = new_scanner.nextRows().join();
      int o_idx = 0; // indices into the rows
      int n_idx = 0;
      
      // counters
      int ok = 0;
      int err = 0;
      ArrayList<KeyValue> last_new = null;
      while (true) {
        System.out.println("---------------------------------");
        byte[] o_key = original_rows.get(o_idx).get(0).key();
        //System.out.println("WORKING: " + Bytes.pretty(o_key));
        long ts = sf.schema.baseTimestamp(o_key);
        NumericSummaryRowSeq o_seq = new NumericSummaryRowSeq(ts, interval);
        for (final KeyValue kv : original_rows.get(o_idx)) {
          //System.out.println("[OLD]: " + Bytes.pretty(kv.qualifier()));
          o_seq.addColumn((byte) 0, kv.qualifier(), kv.value());
        }
        
        while (true) {
          if (o_idx + 1 >= original_rows.size()) {
            original_rows = original_scanner.nextRows().join();
            o_idx = 0;
          } else {
            o_idx++;
          }
          
          if (Bytes.memcmp(original_rows.get(o_idx).get(0).key(), o_key) == 0) {
            for (final KeyValue kv : original_rows.get(o_idx)) {
              //System.out.println("[OLD]: " + Bytes.pretty(kv.qualifier()));
              o_seq.addColumn((byte) 0, kv.qualifier(), kv.value());
            }
          } else {
            break;
          }
        }
        
        // now fetch new
        NumericSummaryRowSeq n_seq = new NumericSummaryRowSeq(ts, interval);
        while (true) {
          if (Bytes.memcmp(new_rows.get(n_idx).get(0).key(), o_key) == 0) {
            for (final KeyValue kv : new_rows.get(n_idx)) {
              //System.out.println("[NEW]: " + Bytes.pretty(kv.qualifier()));
              n_seq.addColumn((byte) 0, kv.qualifier(), kv.value());
            }
          } else {
            break;
          }
          
          if (n_idx + 1 >= new_rows.size()) {
            new_rows = new_scanner.nextRows().join();
            n_idx = 0;
          } else {
            n_idx++;
          }
        }
        
        o_seq.dedupe(false, false);
        n_seq.dedupe(false, false);
        
        NumericSummarySpan o_span = new NumericSummarySpan(false);
        o_span.addSequence(o_seq, true);
        
        NumericSummarySpan n_span = new NumericSummarySpan(false);
        n_span.addSequence(n_seq, true);
        
        Iterator<TimeSeriesValue<?>> o_it = o_span.iterator();
        Iterator<TimeSeriesValue<?>> n_it = n_span.iterator();
        
        TimeSeriesValue<NumericSummaryType> o_v = null;
        TimeSeriesValue<NumericSummaryType> n_v = null;
        boolean failed = false;
        int cntr = 0;
        while (o_it.hasNext()) {
          if (!n_it.hasNext()) {
            System.out.println("[ERROR] New iterator does not have a next value. Last ts: " + 
                (o_v == null ? "Null" : o_v.timestamp().epoch()) + " AT " + cntr);
            err++;
            printDiff(o_seq, n_seq);
            failed = true;
            break;
            //System.exit(1);
          }
          o_v = (TimeSeriesValue<NumericSummaryType>) o_it.next();
          n_v = (TimeSeriesValue<NumericSummaryType>) n_it.next();
          
          if (!o_v.timestamp().compare(Op.EQ, n_v.timestamp())) {
            System.out.println("[ERROR] Timestamps don't match with old: " + o_v.timestamp().epoch() 
                + " new: " + n_v.timestamp().epoch());
            err++;
            printDiff(o_seq, n_seq);
            failed = true;
            break;
          }
          
          for (int summary : o_v.value().summariesAvailable()) {
            //System.out.println("   working summary: " + summary);
            NumericType o_t = o_v.value().value(summary);
            NumericType n_t = n_v.value().value(summary);
            //System.out.println("O: " + o_t + "  N: " + n_t);
            if (o_t == null) {
              if (n_t != null) {
                System.out.println("[ERROR] OT was null but NT was not: " + summary);
                printDiff(o_seq, n_seq);
                err++;
                continue;
              }
            } else if (n_t == null) {
              System.out.println("[ERROR] No summary in new for: " + summary);
              printDiff(o_seq, n_seq);
              err++;
              continue;
            } else if (o_t.isInteger()) {
              if (o_t.longValue() != n_t.longValue()) {
                System.out.println("[ERROR] Different long values for summary: " + summary);
                printDiff(o_seq, n_seq);
                err++;
              }
            } else {
              if (o_t.doubleValue() != n_t.doubleValue()) {
                System.out.println("[ERROR] Different double values for summary: " + summary);
                printDiff(o_seq, n_seq);
                err++;
              }
            }
          }
          cntr++;
        }
        
        if (!failed && n_it.hasNext()) {
          System.out.println("[ERROR] More data in new iterator??");
          printDiff(o_seq, n_seq);
          err++;
        }
        ok++;
        
        // TEMP LIMIT ---------------
        if (ok > 50000) {
          break;
        }
      }

      System.out.println("TOTAL OK: " + ok + "  TOTAL ERR: " + err + "  PCT " + 
          (((double) err / (double) (ok + err)) * (double) 100));
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      System.out.println("WTF?: " + e.getMessage());
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      tsdb.shutdown().join();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("DONE!");
    
    
  }
  
  static void printDiff(final NumericSummaryRowSeq old, final NumericSummaryRowSeq nw) {
    if (old.summary_data.size() != nw.summary_data.size()) {
      System.out.println("[ERR] Old had " + old.summary_data.keySet() + " summaries but new had " + nw.summary_data.keySet());
      return;
    }
    
    Set<Integer> combined = new HashSet(old.summary_data.keySet());
    combined.addAll(nw.summary_data.keySet());
    
    for (final int summary : combined) {
      byte[] o = old.summary_data.get(summary);
      byte[] n = nw.summary_data.get(summary);
      if (o == null) {
        System.out.println("[ERR] Old was missing summary " + summary);
        return;
      }
      if (n == null) {
        System.out.println("[ERR] New was missing summary " + summary);
        return;
      }
      
      System.out.println("Summary: " + summary);
      System.out.println("Old PRE:\n" + Arrays.toString(old.pre.get(summary)) + " \nNew PRE:\n" + Arrays.toString(nw.pre.get(summary)) + "\nMEMCMP: " + Bytes.memcmp(old.pre.get(summary), nw.pre.get(summary)));
      System.out.println("Old array:\n" + Arrays.toString(o) + " \nNew array:\n" + Arrays.toString(n) + "\nMEMCMP: " + Bytes.memcmp(o, n));
      
    }
  }
}

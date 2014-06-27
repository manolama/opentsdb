// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import net.opentsdb.core.Const;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.Internal;
import net.opentsdb.core.Internal.Cell;
import net.opentsdb.core.Query;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

/**
 * Tool to look for and fix corrupted data in a TSDB.
 */
final class Fsck {
  private static final Logger LOG = LoggerFactory.getLogger(Fsck.class);
  
  /** The TSDB to use for access */
  private final TSDB tsdb; 

  /** Options to use while iterating over rows */
  private final FsckOptions options;
  
  /** Counters incremented during processing */
  final AtomicLong kvs_processed = new AtomicLong();
  final AtomicLong rows_processed = new AtomicLong();
  final AtomicLong datapoints = new AtomicLong();
  final AtomicLong annotations = new AtomicLong();
  final AtomicLong bad_key = new AtomicLong();
  final AtomicLong bad_key_fixed = new AtomicLong();
  final AtomicLong duplicates = new AtomicLong();
  final AtomicLong duplicates_fixed = new AtomicLong();
  final AtomicLong orphans = new AtomicLong();
  final AtomicLong orphans_fixed = new AtomicLong();
  final AtomicLong future = new AtomicLong();
  final AtomicLong unknown = new AtomicLong();
  final AtomicLong unknown_fixed = new AtomicLong();
  final AtomicLong bad_values = new AtomicLong();
  final AtomicLong bad_values_deleted = new AtomicLong();
  final AtomicLong value_encoding = new AtomicLong();
  final AtomicLong value_encoding_fixed = new AtomicLong();
  final AtomicLong bad_compacted_columns = new AtomicLong();
  final AtomicLong bad_compacted_columns_deleted = new AtomicLong();
  final AtomicLong vle = new AtomicLong();
  final AtomicLong vle_bytes = new AtomicLong();
  final AtomicLong vle_fixed = new AtomicLong();
  
  /** Length of the metric + timestamp for key validation */
  private final int key_prefix_length;
  
  /** Length of a tagk + tagv pair for key validation */
  private final int key_tags_length;
  
  /**
   * Default Ctor
   * @param tsdb The TSDB to use for access
   * @param options Options to use when iterating over rows
   */
  Fsck(final TSDB tsdb, final FsckOptions options) {
    this.tsdb = tsdb;
    this.options = options;
    key_prefix_length = TSDB.metrics_width() + Const.TIMESTAMP_BYTES;
    key_tags_length = TSDB.tagk_width() + TSDB.tagv_width();
  }
  
  public void runFullTable() throws Exception {
    final long start_time = System.currentTimeMillis() / 1000;
    final long max_id = CliUtils.getMaxMetricID(tsdb);
    
    final int workers = options.threads() > 0 ? options.threads() :
      Runtime.getRuntime().availableProcessors() * 2;
    final double quotient = (double)max_id / (double)workers;
    LOG.info("Max metric ID is [" + max_id + "]");
    LOG.info("Spooling up [" + workers + "] worker threads");
    long index = 1;
    final Thread[] threads = new Thread[workers];
    for (int i = 0; i < workers; i++) {
      threads[i] = new FsckWorker(index, quotient, i);
      threads[i].setName("Fsck # " + i);
      threads[i].start();
      index += quotient;
      if (index < max_id) {
        index++;
      }
    }
 
    // wait till we're all done
    for (int i = 0; i < workers; i++) {
      threads[i].join();
      LOG.info("[" + i + "] Finished");
    }
    
    // make sure buffered data is flushed to storage before exiting
    tsdb.flush().joinUninterruptibly();
    
    LOG.info("Key Values Processed: " + kvs_processed.get());

    
    final long duration = (System.currentTimeMillis() / 1000) - start_time;
    LOG.info("Completed fsck in [" + duration + "] seconds");
  }
  
  long totalErrors() {
    return bad_key.get() + duplicates.get() + orphans.get() + unknown.get() +
        bad_values.get() + bad_compacted_columns.get() + value_encoding.get();
  }
  
  long totalFixed() {
    return bad_key_fixed.get() + duplicates_fixed.get() + orphans_fixed.get() +
        unknown_fixed.get() + value_encoding_fixed.get() + bad_values_deleted.get();
  }
  
  long correctable() {
    return bad_key.get() + duplicates.get() + orphans.get() + unknown.get() +
        bad_values.get() + bad_compacted_columns.get() + value_encoding.get();
  }
  
  final class FsckWorker extends Thread {
    final long start_id;
    final long end_id;
    final int thread_id;
    final Query query;
    final Set<byte[]> tsuids = new HashSet<byte[]>();
    /**
     * Default Ctor
     * @param start_id
     * @param end_id
     * @param thread_id
     */
    FsckWorker(final long start_id, final double quotient, final int thread_id) {
      this.start_id = start_id;
      this.end_id = start_id + (long) quotient + 1; // teensy bit of overlap
      this.thread_id = thread_id;
      query = null;
    }
    
    FsckWorker(final Query query, final int thread_id) {
      start_id = 0;
      end_id = 0;
      this.thread_id = thread_id;
      this.query = query;
    }
    
    public void run() {
      final Scanner scanner = query != null ? Internal.getScanner(query) :
        CliUtils.getDataTableScanner(tsdb, start_id, end_id);
      
      ArrayList<ArrayList<KeyValue>> rows;
      try {
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          for (final ArrayList<KeyValue> row : rows) {
            fsckRow(row);
            kvs_processed.addAndGet(row.size());
          }
          rows_processed.addAndGet(rows.size());
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    /**
     * Do the work!
     * @param row
     */
    private void fsckRow(final ArrayList<KeyValue> row) throws Exception {
      // The data table should contain only rows with a metric, timestamp and
      // one or more tag pairs. Future version may use different prefixes or 
      // key formats but for now, we can safely delete any rows with invalid 
      // keys.
      if (!fsckKey(row.get(0).key())) {
        return;
      }
      
      int single_datapoints = 0;
      int total_datapoints = 0;
      int compacted_columns = 0;
      final List<Deferred<Object>> requests = new ArrayList<Deferred<Object>>();
      
      // store every data point for the row in here 
      final TreeMap<Long, ArrayList<DP>> previous = 
        new TreeMap<Long, ArrayList<DP>>();
      
      for (final KeyValue kv : row) {
        // these are not final as they may be modified when fixing is enabled
        byte[] value = kv.value(); 
        byte[] qual = kv.qualifier();
        
        // all qualifiers must be at least 2 bytes long, i.e. a single data point
        if (qual.length < 2) {
          unknown.getAndIncrement();
          LOG.error("Invalid qualifier, must be on 2 bytes or more.\n\t"
                    + kv);
          if (options.fix() && options.deleteUnknownColumns()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
            requests.add(tsdb.getClient().delete(delete));
            unknown_fixed.getAndIncrement();
          }
          continue;
        }
        
        // All data point columns have an even number of bytes, so if we find
        // one that has an odd length, it could be an OpenTSDB  non-data point 
        // object or it could be something else.
        if (qual.length % 2 != 0) {
          // If this test fails, the column is not non-data point object such
          // as an annotation or blob. Future versions may be able to compact 
          // these objects but for now we'll consider it an error.
          if (qual.length != 3 && qual.length != 5) {
            unknown.getAndIncrement();
            LOG.error("Unknown qualifier, must be 2, 3, 5 or an even number " +
                "of bytes.\n\t" + kv);
            if (options.fix() && options.deleteUnknownColumns()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              requests.add(tsdb.getClient().delete(delete));
              unknown_fixed.getAndIncrement();
            }
            continue;
          }
          
          // TODO - create a list of non-data point objects and iterate over them
          // TODO - perform validation of the annotation
          if (qual[0] == Annotation.PREFIX()) {
            annotations.getAndIncrement();
            continue;
          }
          LOG.warn("Found an object possibly from a future version of OpenTSDB\n\t"
              + kv);
          future.getAndIncrement();
          continue;
        }
        
        // This is (hopefully) a compacted column with multiple data points. It 
        // could have two points with second qualifiers or multiple points with
        // a mix of second and millisecond qualifiers
        if (qual.length == 4 && !Internal.inMilliseconds(qual[0])
            || qual.length > 4) {
          if (value[value.length - 1] > Const.MS_MIXED_COMPACT) {
            // TODO - figure out a way to fix these. Maybe lookup a row before 
            // or after and try parsing this for values. If the values are
            // somewhat close to the others, then we could just set the last
            // byte. Otherwise we'd need to toss it.
            bad_compacted_columns.getAndIncrement();
            LOG.error("The last byte of a compacted should be 0 or 1. Either"
                      + " this value is corrupted or it was written by a"
                      + " future version of OpenTSDB.\n\t" + kv);
            continue;
          }
          
          // add every cell in the compacted column to the previously seen
          // data point tree so that we can scan for duplicate timestamps
          try {
            final ArrayList<Cell> cells = Internal.extractDataPoints(kv);
            for (final Cell cell : cells) {
              final long offset = 
                  Internal.getOffsetFromQualifier(cell.qualifier());
              ArrayList<DP> dps = previous.get(offset);
              if (dps == null) {
                dps = new ArrayList<DP>(1);
                previous.put(offset, dps);
              }
              dps.add(new DP(kv.timestamp(), kv.qualifier(), true));
              total_datapoints++;
            }
          } catch (IllegalDataException e) {
            bad_compacted_columns.getAndIncrement();
            LOG.error(e.getMessage());
            if (options.fix() && options.deleteBadCompacts()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              requests.add(tsdb.getClient().delete(delete));
              bad_compacted_columns_deleted.getAndIncrement();
            }
          }
          compacted_columns++;
          continue;
        }
        
        // at this point we're dealing with a single data point encoded in 
        // seconds or milliseconds.
        final long offset = Internal.getOffsetFromQualifier(qual);
        ArrayList<DP> dps = previous.get(offset);
        if (dps == null) {
          dps = new ArrayList<DP>(1);
          previous.put(offset, dps);
        }
        dps.add(new DP(kv.timestamp(), kv.qualifier(), false));
        single_datapoints++;
        total_datapoints++;
        
        if (value.length > 8) {
          bad_values.getAndIncrement();
          LOG.error("Value more than 8 bytes long with a " 
              + kv.qualifier().length + "-byte qualifier.\n\t" + kv);
          if (options.fix() && options.deleteBadValues()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
            requests.add(tsdb.getClient().delete(delete));
            bad_values_deleted.getAndIncrement();
          }
          continue;
        }
        
        // validate the floating point length and check for errors from old
        // versions of OpenTSDB
        if (Internal.getFlagsFromQualifier(qual) == (Const.FLAG_FLOAT | 0x3)) {  // float | 4 bytes
          // The qualifier says the value is on 4 bytes, and the value is
          // on 8 bytes, then the 4 MSBs must be 0s.  Old versions of the
          // code were doing this.  It's kinda sad.  Some versions had a
          // bug whereby the value would be sign-extended, so we can
          // detect these values and fix them here.
          if (value.length == 8) {
            if (value[0] == -1 && value[1] == -1
                && value[2] == -1 && value[3] == -1 && qual.length == 2) {
              value_encoding.getAndIncrement();
              LOG.error("Floating point value with 0xFF most significant"
                  + " bytes, probably caused by sign extension bug"
                  + " present in revisions [96908436..607256fc].\n"
                  + "\t" + kv);
              if (options.fix()) {
                final float value_as_float =
                    Float.intBitsToFloat(Bytes.getInt(value, 4));
                value = Bytes.fromInt(
                    Float.floatToRawIntBits((float)value_as_float));
                final PutRequest put = new PutRequest(tsdb.dataTable(), 
                    kv.key(), kv.family(), qual, value);
                requests.add(tsdb.getClient().put(put));
                value_encoding_fixed.getAndIncrement();
              }
            } else if (value[0] != 0 || value[1] != 0
                       || value[2] != 0 || value[3] != 0) {
              LOG.error("Floating point value was marked as 4 bytes long but"
                  + " was actually 8 bytes long and the first four bytes were"
                  + " not zeroed\n\t" + kv);
              bad_values.getAndIncrement();
              if (options.fix() && options.deleteBadValues()) {
                final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
                requests.add(tsdb.getClient().delete(delete));
                bad_values_deleted.getAndIncrement();
              }
            } else {
              LOG.warn("Floating point value was marked as 4 bytes long but"
                  + " was actually 8 bytes long\n\t" + kv);
              value_encoding.getAndIncrement();
              if (options.fix()) {
                final float value_as_float =
                    Float.intBitsToFloat(Bytes.getInt(value, 4));
                value = Bytes.fromInt(
                    Float.floatToRawIntBits((float)value_as_float));
                final PutRequest put = new PutRequest(tsdb.dataTable(), 
                    kv.key(), kv.family(), qual, value);
                requests.add(tsdb.getClient().put(put));
                value_encoding_fixed.getAndIncrement();
              }
            }
          } else if (value.length != 4) {
            bad_values.getAndIncrement();
            LOG.error("This floating point value must be encoded either on"
                      + " 4 or 8 bytes, but it's on " + value.length
                      + " bytes.\n\t" + kv);
            if (options.fix() && options.deleteBadValues()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              requests.add(tsdb.getClient().delete(delete));
              bad_values_deleted.getAndIncrement();
            }
          }
          
          continue;
        } else {
          // this should be a single integer value. Check the encoding to make
          // sure it's the proper length, and if the flag is set to fix encoding
          // we can save space with VLE.
          final byte length = Internal.getValueLengthFromQualifier(qual);
          if (value.length != length) {
            bad_values.getAndIncrement();
            LOG.error("The integer value is " + value.length + " bytes long but "
                + "should be " + length + " bytes.\n\t" + kv);
            if (options.fix() && options.deleteBadValues()) {
              final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), kv);
              requests.add(tsdb.getClient().delete(delete));
              bad_values_deleted.getAndIncrement();
            }
          }
          
          // OpenTSDB had support for VLE decoding of integers but only wrote
          // on 8 bytes at the start. Lets see how much space we could save. 
          // We'll assume that a length other than 8 bytes is already VLE'd
          if (length == 8) {
            final long decoded = Bytes.getLong(value);
            boolean fixup = false;
            if (Byte.MIN_VALUE <= decoded && decoded <= Byte.MAX_VALUE) {
              vle.getAndIncrement();
              vle_bytes.addAndGet(7);
              fixup = true;
              value = new byte[] { (byte) decoded };
            } else if (Short.MIN_VALUE <= decoded && decoded <= Short.MAX_VALUE) {
              vle.getAndIncrement();
              vle_bytes.addAndGet(6);
              fixup = true;
              value = Bytes.fromShort((short) decoded);
            } else if (Integer.MIN_VALUE <= decoded && 
                decoded <= Integer.MAX_VALUE) {
              vle.getAndIncrement();
              vle_bytes.addAndGet(4);
              fixup = true;
              value = Bytes.fromInt((int) decoded);
            } // else it needs 8 bytes, it's on 8 bytes, yipee
            
            if (fixup && options.fix() && options.vle()) {
              qual[qual.length - 1] &= 0xF0 | (value.length - 1);
              final PutRequest put = new PutRequest(tsdb.dataTable(), 
                  kv.key(), kv.family(), qual, value);
              requests.add(tsdb.getClient().put(put));
              vle_fixed.getAndIncrement();
            }
          }
        }        
      } // end key value loop
      
      // iterate over the data points and see if we had any duplicates
//      for (Map.Entry<Long, ArrayList<DP>> offset_map : previous.entrySet()) {
//        if (offset_map.getValue().size() < 2) {
//          continue;
//        }
//        
//        // for now, delete the non-compacted dupes
//        int compacted = 0;
//        long earliest_value = Long.MAX_VALUE;
//        for (DP dp : offset_map.getValue()) {
//          if (dp.compacted) {
//            compacted++;
//          }
//          if (dp.timestamp < earliest_value) {
//            earliest_value = dp.timestamp;
//          }
//        }
//        
//        // if there are more than one compacted columns with the same
//        // timestamp, something went pear shaped and we need more work to
//        // figure out what to do
//        final StringBuilder buf = new StringBuilder();
//        if (compacted > 1) {
//          errors++;
//          buf.setLength(0);
//          buf.append("More than one compacted column had a value for the same timestamp: ")
//             .append("timestamp: (")
//             .append(time_map.getKey())
//             .append(")\n");
//          for (DP dp : time_map.getValue()) {
//            buf.append("    ")
//               .append(Arrays.toString(dp.qualifier))
//               .append("\n");
//          }
//          LOG.error(buf.toString());
//        } else {
//          errors++;
//          correctable++;
//          if (fix) {
//            if (compacted < 1) {
//              // keep the earliest value
//              boolean matched = false;
//              for (DP dp : time_map.getValue()) {
//                if (dp.stored_timestamp == earliest_value && !matched) {
//                  matched = true;
//                  continue;
//                }
//                final DeleteOutOfOrder delooo = 
//                  new DeleteOutOfOrder(row.get(0).key(), 
//                      "t".getBytes(), dp.qualifier);
//                delooo.call(null);
//              }
//            } else {
//              // keep the compacted value
//              for (DP dp : time_map.getValue()) {
//                if (dp.compacted) {
//                  continue;
//                }
//                
//                final DeleteOutOfOrder delooo = 
//                  new DeleteOutOfOrder(row.get(0).key(),
//                      "t".getBytes(), dp.qualifier);
//                delooo.call(null);
//              }
//            }
//          } else {
//            buf.setLength(0);
//            buf.append("More than one column had a value for the same timestamp: ")
//               .append("timestamp: (")
//               .append(time_map.getKey())
//               .append(")\n");
//            for (DP dp : time_map.getValue()) {
//              buf.append("    ")
//                 .append(Arrays.toString(dp.qualifier))
//                 .append("\n");
//            }
//            LOG.error(buf.toString());
//          }
//        }
//      }
      
      // TODO compact if we have 
    }
    
    private boolean fsckKey(final byte[] key) throws Exception {
      if (key.length < key_prefix_length || 
          (key.length - key_prefix_length) % key_tags_length != 0) {
        LOG.error("Invalid row key.\n\tKey: " + UniqueId.uidToString(key));
        bad_key.getAndIncrement();
        
        if (options.fix() && options.deleteBadKeys()) {
          final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
          tsdb.getClient().delete(delete);
          bad_key_fixed.getAndIncrement();
        }
        return false;
      }
      
      // Process the time series ID by resolving the UIDs to names if we haven't
      // already seen this particular TSUID
      final byte[] tsuid = UniqueId.getTSUIDFromKey(key, TSDB.metrics_width(), 
          Const.TIMESTAMP_BYTES);
      if (!tsuids.contains(tsuid)) {
        try {
          RowKey.metricNameAsync(tsdb, key).joinUninterruptibly();
        } catch (NoSuchUniqueId nsui) {
          LOG.error("Unable to resolve the metric from the row key.\n\tKey: "
              + UniqueId.uidToString(key) + "\n\t" + nsui.getMessage());
          orphans.getAndIncrement();
          
          if (options.fix() && options.deleteOrphans()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
            tsdb.getClient().delete(delete);
            orphans_fixed.getAndIncrement();
          }
          return false;
        }
        
        try {
          Tags.resolveIds(tsdb, (ArrayList<byte[]>)
              UniqueId.getTagPairsFromTSUID(tsuid));
        } catch (NoSuchUniqueId nsui) {
          LOG.error("Unable to resolve the a tagk or tagv from the row key.\n\tKey: "
              + UniqueId.uidToString(key) + "\n\t" + nsui.getMessage());
          orphans.getAndIncrement();
          
          if (options.fix() && options.deleteOrphans()) {
            final DeleteRequest delete = new DeleteRequest(tsdb.dataTable(), key);
            tsdb.getClient().delete(delete);
            orphans_fixed.getAndIncrement();
          }
          return false;
        }
      }
      return true;
    }
  }
  
  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: fsck"
        + " [--fix] START-DATE [END-DATE] query [queries...]\n"
        + "To see the format in which queries should be written, see the help"
        + " of the 'query' command.\n"
        + "The --fix flag will attempt to fix errors,"
        + " but be careful when using it.");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  /**
   * The main class executed from the "tsdb" script
   * @param args Command line arguments to parse
   * @throws Exception If something goes pear shaped
   */
  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    FsckOptions.addDataOptions(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);
    final FsckOptions options = new FsckOptions(argp, config);
    final TSDB tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    final byte[] table = config.getString("tsd.storage.hbase.data_table").getBytes(); 
    argp = null;
    int errors = 42;
    try {
      errors = fsck(tsdb, tsdb.getClient(), table, options, args);
    } finally {
      tsdb.shutdown().joinUninterruptibly();
    }
    System.exit(errors == 0 ? 0 : 1);
  }

  private static int fsck(final TSDB tsdb,
                           final HBaseClient client,
                           final byte[] table,
                           final FsckOptions options,
                           final String[] args) throws Exception {

    /** Callback to asynchronously delete a specific {@link KeyValue}.  */
    final class DeleteOutOfOrder implements Callback<Deferred<Object>, Object> {

        private final KeyValue kv;

        public DeleteOutOfOrder(final KeyValue kv) {
          this.kv = kv;
        }

        public DeleteOutOfOrder(final byte[] key, final byte[] family, 
            final byte[] qualifier) {
          this.kv = new KeyValue(key, family, qualifier, new byte[0]);
        }
        
        public Deferred<Object> call(final Object arg) {
          return client.delete(new DeleteRequest(table, kv.key(),
                                                 kv.family(), kv.qualifier()));
        }

        public String toString() {
          return "delete out-of-order data";
        }
      }
    
    int errors = 0;
    int correctable = 0;

    final short metric_width = Internal.metricWidth(tsdb);

    final ArrayList<Query> queries = new ArrayList<Query>();
    CliQuery.parseCommandLineQuery(args, tsdb, queries, null, null);
    final StringBuilder buf = new StringBuilder();
    for (final Query query : queries) {
      final long start_time = System.nanoTime();
      long ping_start_time = start_time;
      LOG.info("Starting to fsck data covered by " + query);
      long kvcount = 0;
      long rowcount = 0;
      final Bytes.ByteMap<Seen> seen = new Bytes.ByteMap<Seen>();
      final Scanner scanner = Internal.getScanner(query);
      ArrayList<ArrayList<KeyValue>> rows;
      
      // store every data point for the row in here 
      final TreeMap<Long, ArrayList<DP>> previous = 
        new TreeMap<Long, ArrayList<DP>>();
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          rowcount++;
          previous.clear();
          
          // Take a copy of the row-key because we're going to zero-out the
          // timestamp and use that as a key in our `seen' map.
          final byte[] key = row.get(0).key().clone();
          final long base_time = Bytes.getUnsignedInt(key, metric_width);
          for (int i = metric_width; i < metric_width + Const.TIMESTAMP_BYTES; i++) {
            key[i] = 0;
          }
          Seen prev = seen.get(key);
          if (prev == null) {
            prev = new Seen(base_time - 1, row.get(0));
            seen.put(key, prev);
          }
          for (final KeyValue kv : row) {
            kvcount++;
            if (kvcount % 100000 == 0) {
              final long now = System.nanoTime();
              ping_start_time = (now - ping_start_time) / 1000000;
              LOG.info("... " + kvcount + " KV analyzed in " + ping_start_time
                       + "ms (" + (100000 * 1000 / ping_start_time) + " KVs/s)");
              ping_start_time = now;
            }
            byte[] value = kv.value();
            final byte[] qual = kv.qualifier();
            if (qual.length < 2) {
              errors++;
              LOG.error("Invalid qualifier, must be on 2 bytes or more.\n\t"
                        + kv);
              continue;
            } else if (qual.length % 2 != 0) {
              if (qual.length != 3 && qual.length != 5) {
                errors++;
                LOG.error("Found unknown column in row.\n\t" + kv);
                continue;
              }
              
              // check for known types using the prefix. If the type is unknown
              // it could just be from a future version so don't flag it as an
              // error. Log it via debugging.
              if (qual[0] == Annotation.PREFIX()) {
                continue;
              }
              LOG.debug("Found an object from a future version of OpenTSDB\n\t" 
                  + kv);
              continue;
            } else if (qual.length == 4 && !Internal.inMilliseconds(qual[0])
                || qual.length > 4) {
              // compacted row
              if (value[value.length - 1] > Const.MS_MIXED_COMPACT) {
                errors++;
                LOG.error("The last byte of a compacted should be 0 or 1. Either"
                          + " this value is corrupted or it was written by a"
                          + " future version of OpenTSDB.\n\t" + kv);
                continue;
              }
              
              // add every cell in the compacted column to the previously seen
              // data point tree so that we can scan for duplicate timestamps
              try {
                final ArrayList<Cell> cells = Internal.extractDataPoints(kv); 
                for (Cell cell : cells) {
                  final long ts = cell.timestamp(base_time);
                  ArrayList<DP> dps = previous.get(ts);
                  if (dps == null) {
                    dps = new ArrayList<DP>(1);
                    previous.put(ts, dps);
                  }
                  dps.add(new DP(kv.timestamp(), kv.qualifier(), true));
                }
              } catch (IllegalDataException e) {
                errors++;
                LOG.error(e.getMessage());
              }
              
              // TODO - validate the compaction
              continue;
            } // else: qualifier is on 2 or 4 bytes, it's an individual value.

            final long timestamp = 
              Internal.getTimestampFromQualifier(qual, base_time);
            ArrayList<DP> dps = previous.get(timestamp);
            if (dps == null) {
              dps = new ArrayList<DP>(1);
              previous.put(timestamp, dps);
            }
            dps.add(new DP(kv.timestamp(), kv.qualifier(), false));
            
            if (value.length > 8) {
              errors++;
              LOG.error("Value more than 8 byte long with a " 
                        + kv.qualifier().length + "-byte qualifier.\n\t" + kv);
            }
            // TODO(tsuna): Don't hardcode 0x8 / 0x3 here.
            if (qual.length == 2 && 
                Internal.getFlagsFromQualifier(qual) == (0x8 | 0x3)) {  // float | 4 bytes
              // The qualifier says the value is on 4 bytes, and the value is
              // on 8 bytes, then the 4 MSBs must be 0s.  Old versions of the
              // code were doing this.  It's kinda sad.  Some versions had a
              // bug whereby the value would be sign-extended, so we can
              // detect these values and fix them here.
              if (value.length == 8) {
                if (value[0] == -1 && value[1] == -1
                    && value[2] == -1 && value[3] == -1) {
                  errors++;
                  correctable++;
                  if (options.fix()) {
                    value = value.clone();  // We're going to change it.
                    value[0] = value[1] = value[2] = value[3] = 0;
                    client.put(new PutRequest(table, kv.key(), kv.family(),
                                              qual, value));
                  } else {
                    LOG.error("Floating point value with 0xFF most significant"
                              + " bytes, probably caused by sign extension bug"
                              + " present in revisions [96908436..607256fc].\n"
                              + "\t" + kv);
                  }
                } else if (value[0] != 0 || value[1] != 0
                           || value[2] != 0 || value[3] != 0) {
                  errors++;
                }
              } else if (value.length != 4) {
                errors++;
                LOG.error("This floating point value must be encoded either on"
                          + " 4 or 8 bytes, but it's on " + value.length
                          + " bytes.\n\t" + kv);
              }
            }
          }

          // scan for dupes
          for (Map.Entry<Long, ArrayList<DP>> time_map : previous.entrySet()) {
            if (time_map.getValue().size() < 2) {
              continue;
            }
            
            // for now, delete the non-compacted dupes
            int compacted = 0;
            long earliest_value = Long.MAX_VALUE;
            for (DP dp : time_map.getValue()) {
              if (dp.compacted) {
                compacted++;
              }
              if (dp.timestamp < earliest_value) {
                earliest_value = dp.timestamp;
              }
            }
            
            // if there are more than one compacted columns with the same
            // timestamp, something went pear shaped and we need more work to
            // figure out what to do
            if (compacted > 1) {
              errors++;
              buf.setLength(0);
              buf.append("More than one compacted column had a value for the same timestamp: ")
                 .append("timestamp: (")
                 .append(time_map.getKey())
                 .append(")\n");
              for (DP dp : time_map.getValue()) {
                buf.append("    ")
                   .append(Arrays.toString(dp.qualifier))
                   .append("\n");
              }
              LOG.error(buf.toString());
            } else {
              errors++;
              correctable++;
              if (options.fix()) {
                if (compacted < 1) {
                  // keep the earliest value
                  boolean matched = false;
                  for (DP dp : time_map.getValue()) {
                    if (dp.timestamp == earliest_value && !matched) {
                      matched = true;
                      continue;
                    }
                    final DeleteOutOfOrder delooo = 
                      new DeleteOutOfOrder(row.get(0).key(), 
                          "t".getBytes(), dp.qualifier);
                    delooo.call(null);
                  }
                } else {
                  // keep the compacted value
                  for (DP dp : time_map.getValue()) {
                    if (dp.compacted) {
                      continue;
                    }
                    
                    final DeleteOutOfOrder delooo = 
                      new DeleteOutOfOrder(row.get(0).key(),
                          "t".getBytes(), dp.qualifier);
                    delooo.call(null);
                  }
                }
              } else {
                buf.setLength(0);
                buf.append("More than one column had a value for the same timestamp: ")
                   .append("timestamp: (")
                   .append(time_map.getKey())
                   .append(")\n");
                for (DP dp : time_map.getValue()) {
                  buf.append("    ")
                     .append(Arrays.toString(dp.qualifier))
                     .append("\n");
                }
                LOG.error(buf.toString());
              }
            }
          }
        }
      }
      final long timing = (System.nanoTime() - start_time) / 1000000;
      System.out.println(kvcount + " KVs (in " + rowcount
                         + " rows) analyzed in " + timing
                         + "ms (~" + (kvcount * 1000 / timing) + " KV/s)");
    }

    System.out.println(errors != 0 ? "Found " + errors + " errors."
                       : "No error found.");
    if (!options.fix() && correctable > 0) {
      System.out.println(correctable + " of these errors are automatically"
                         + " correctable, re-run with --fix.\n"
                         + "Make sure you understand the errors above and you"
                         + " know what you're doing before using --fix.");
    }
    return errors;
  }
  
  /**
   * Internal class used for examining data points in a row to determine if
   * we have any duplicates. Can then be used to delete the duplicate columns.
   */
  private static final class DP {
    
    long timestamp;
    byte[] qualifier;
    boolean compacted;
    
    DP(final long timestamp, final byte[] qualifier, final boolean compacted) {
      this.timestamp = timestamp;
      this.qualifier = qualifier;
      this.compacted = compacted;
    }
  }
  
  /**
   * The last data point we've seen for a particular time series.
   */
  private static final class Seen {
    /** A 32-bit unsigned integer that holds a UNIX timestamp in milliseconds.  */
    private long timestamp;
    /** The raw data point (or points if the KV contains more than 1).  */
    KeyValue kv;

    private Seen(final long timestamp, final KeyValue kv) {
      this.timestamp = timestamp;
      this.kv = kv;
    }

    /** Returns the UNIX timestamp (in seconds) as a 32-bit unsigned int.  */
    public long timestamp() {
      return timestamp;
    }

    /** Updates the UNIX timestamp (in seconds) with a 32-bit unsigned int.  */
    public void setTimestamp(final long timestamp) {
      this.timestamp = timestamp;
    }
  }

}

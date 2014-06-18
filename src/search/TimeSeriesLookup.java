// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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
package net.opentsdb.search;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.Const;
import net.opentsdb.core.RowKey;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.ByteArrayPair;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lookup series related to a metric, tagk, tagv or any combination thereof.
 * This class doesn't handle wild-card searching yet.
 * @since 2.1
 */
public class TimeSeriesLookup {
  private static final Logger LOG = 
      LoggerFactory.getLogger(TimeSeriesLookup.class);
  
  /** The metric to iterate over, may be null */
  private final String metric;
  
  /** Optional tags to match on, may be null */
  private final List<Map.Entry<String, String>> tags;
  
  /** Whether or not to use the tsdb-meta table for lookups. Defaults to true */
  private boolean use_meta;
  
  /** Whether or not to dump the output to standard out for CLI commands */
  private boolean to_stdout;
  
  /** The TSD to use for lookups */
  private final TSDB tsdb;
  
  /**
   * Default ctor
   * @param tsdb The TSD to which we belong
   * @param metric A metric to match on, may be null
   * @param tags One or more tags to match on, may be null
   */
  public TimeSeriesLookup(final TSDB tsdb, final String metric, 
      final List<Map.Entry<String, String>> tags) {
    this.tsdb = tsdb;
    this.metric = metric;
    this.tags = tags;
    use_meta = true;
  }
  
  /**
   * Lookup time series associated with the given metric, tagk, tagv or tag 
   * pairs. Either the meta table or the data table will be scanned. If no
   * metric is given, a full table scan must be performed and this call may take
   * a long time to complete. 
   * When dumping to stdout, if an ID can't be looked up, it will be logged and
   * skipped.
   * @return A list of TSUIDs matching the given lookup query.
   * @throws NoSuchUniqueName if any of the given names fail to resolve to a 
   * UID.
   */
  public List<byte[]> lookup() {
    final Scanner scanner = getScanner();
    final List<byte[]> tsuids = new ArrayList<byte[]>();
    // we don't really know what size the UIDs will resolve to so just grab
    // a decent amount.
    final StringBuffer buf = to_stdout ? new StringBuffer(2048) : null;
    ArrayList<ArrayList<KeyValue>> rows;
    
    try {
      // synchronous to avoid stack overflows when scanning across the main data
      // table.
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] tsuid = use_meta ? row.get(0).key() : 
            UniqueId.getTSUIDFromKey(row.get(0).key(), TSDB.metrics_width(), 
                Const.TIMESTAMP_BYTES);
          
          if (to_stdout) {
            try {
              buf.append(UniqueId.uidToString(tsuid)).append(" ");
              buf.append(RowKey.metricNameAsync(tsdb, tsuid)
                  .joinUninterruptibly());
              buf.append(" ");
              
              final List<byte[]> tag_ids = UniqueId.getTagPairsFromTSUID(tsuid);
              final Map<String, String> resolved_tags = 
                  Tags.resolveIdsAsync(tsdb, tag_ids).joinUninterruptibly();
              for (final Map.Entry<String, String> tag_pair : 
                  resolved_tags.entrySet()) {
                buf.append(tag_pair.getKey()).append("=")
                   .append(tag_pair.getValue()).append(" ");
              }
              System.out.println(buf.toString());
            } catch (NoSuchUniqueId nsui) {
              LOG.error("Unable to resolve UID in TSUID (" + 
                  UniqueId.uidToString(tsuid) + ") " + nsui.getMessage());
            }
            buf.setLength(0);   // reset the buffer so we can re-use it
          } else {
            tsuids.add(tsuid);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    } finally {
      scanner.close();
    }
    
    return tsuids;
  }
  
  /**
   * Configures the scanner for iterating over the meta or data tables. If the
   * metric has been set, then we scan a small slice of the table where the 
   * metric lies, otherwise we have to scan the whole table. If tags are 
   * given then we setup a row key regex
   * @return A configured scanner
   */
  protected Scanner getScanner() {
    final Scanner scanner = tsdb.getClient().newScanner(
        use_meta ? tsdb.metaTable() : tsdb.dataTable());
    
    // if a metric is given, we need to resolve it's UID and set the start key
    // to the UID and the stop key to the next row by incrementing the UID. 
    if (metric != null && !metric.isEmpty()) {
      final byte[] metric_uid = tsdb.getUID(UniqueIdType.METRIC, metric);
      LOG.debug("Found UID (" + UniqueId.uidToString(metric_uid) + 
        ") for metric (" + metric + ")");
      scanner.setStartKey(metric_uid);
      long uid = UniqueId.uidToLong(metric_uid, TSDB.metrics_width());
      uid++; // TODO - see what happens when this rolls over
      scanner.setStopKey(UniqueId.longToUID(uid, TSDB.metrics_width()));
    } else {
      LOG.debug("Performing full table scan, no metric provided");
    }
    
    // When building the regex, we can search for tagks, tagvs or pairs. Thus:
    // val, null <- lookup all series with a tagk
    // val, val  <- lookup all series with a tag pair
    // null, val <- lookup all series with a tag value somewhere
    if (tags != null && !tags.isEmpty()) {
      final List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(tags.size());
      for (Map.Entry<String, String> tag : tags) {
        final byte[] tagk = tag.getKey() != null ? 
            tsdb.getUID(UniqueIdType.TAGK, tag.getKey()) : null;
        final byte[] tagv = tag.getValue() != null ?
            tsdb.getUID(UniqueIdType.TAGV, tag.getValue()) : null;
        pairs.add(new ByteArrayPair(tagk, tagv));
      }
      // remember, tagks are sorted in the row key so we need to supply a sorted
      // regex or matching will fail.
      Collections.sort(pairs);
      
      final short name_width = TSDB.tagk_width();
      final short value_width = TSDB.tagv_width();
      final short tagsize = (short) (name_width + value_width);
      
      // our regex may wind up something like this:
      //(?s)^.{3}(?:.{6})*\\Q\000\000\001\000\000\003\\E(?:.{6})*
      //                  \\Q\000\000\002\\E(?:.{3})+(?:.{6})*$
      final StringBuilder buf = new StringBuilder(
          22  // "^.{N}" + "(?:.{M})*" + "$" + wiggle
          + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
             * (pairs.size())));
      buf.append("(?s)^.{").append(TSDB.metrics_width())
         .append("}");
      if (!use_meta) {
        buf.append("(?:.{").append(Const.TIMESTAMP_BYTES).append("})*");
      }
      buf.append("(?:.{").append(tagsize).append("})*");
      
      for (final ByteArrayPair pair : pairs) {
        if (pair.getKey() != null && pair.getValue() != null) {
          buf.append("\\Q");
          addId(buf, pair.getKey());
          addId(buf, pair.getValue());
          buf.append("\\E");
        } else if (pair.getKey() == null) {
          buf.append("(?:.{3})+");
          buf.append("\\Q");
          addId(buf, pair.getValue());
          buf.append("\\E");
        } else {
          buf.append("\\Q");
          addId(buf, pair.getKey());
          buf.append("\\E");
          buf.append("(?:.{3})+");
        }
        buf.append("(?:.{6})*"); // catch tag pairs in between
      }
      buf.append("$");
      
      scanner.setKeyRegexp(buf.toString(), Charset.forName("ISO-8859-1"));
    }
    return scanner;
  }
  
  /**
   * Appends the given ID to the given buffer, escaping where appropriate
   * @param buf The string buffer to append to
   * @param id The ID to append
   */
  private static void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
  }

  /** @param use_meta Whether we use the meta or data tables for lookups */
  public void setUseMeta(final boolean use_meta) {
    this.use_meta = use_meta;
  }
  
  /** @param to_stdout Whether or not to dump to standard out as we scan */
  public void setToStdout(final boolean to_stdout) {
    this.to_stdout = to_stdout;
  }
}

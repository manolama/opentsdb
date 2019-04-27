// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.openhft.hashing.LongHashFunction;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.PooledPSTRunnable;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.TSUID;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Exceptions;

/**
 * A single scanner for a single metric within a single salt bucket 
 * (optionally). 
 * <p>
 * While the most efficient scanner is one with a fully configured
 * start and stop key and no {@link Tsdb1xScanners#scannerFilter()}, if 
 * filters are present, then it will resolve the UIDs of the rows into 
 * the string IDs, then filter them and cache the results in sets.
 * <p>
 * If {@link Tsdb1xScanners#sequenceEnd()} is reached or 
 * {@link Tsdb1xScanners#isFull()} is returned, then the scanner can stop
 * mid-result and buffer some data till {@link #fetchNext(Tsdb1xQueryResult, Span)}
 * is called again.
 * <p>
 * When resolving filters, it's possible to ignore UIDs that fail to
 * resolve to a name by setting the {@link #skip_nsui} flag.
 * 
 * @since 3.0
 */
public class Tsdb1xScanner2 {
  private static final Logger LOG = LoggerFactory.getLogger(Tsdb1xScanner2.class);

  private static final byte NUMERIC_TYPE = (byte) 1;
  private static final byte NUMERIC_PREFIX = (byte) 0;
  
  /** The scanner owner to report to. */
  private final Tsdb1xScanners2 owner;
  
  /** The actual HBase scanner to execute. */
  private final Scanner scanner;
  
  /** The 0 based index amongst salt buckets. */
  private final int idx;
  
  /** An optional rollup interval. */
  private final RollupInterval rollup_interval;
  
  /** The current state of this scanner. */
  private State state;
  
  /** When filtering, used to hold the TSUIDs being resolved. */
  protected TLongObjectMap<ResolvingId> keys_to_ids;
  
  /** The set of TSUID hashes that we have resolved and have failed our
   * filter set. */
  protected TLongSet skips;
  
  /** The set of TSUID hashes that we have resolved and matched our filter
   * set. */
  protected TLongSet keepers;
  
  /** A singleton base timestamp for this scanner. */
  protected TimeStamp last_ts;
  protected TimeStamp base_ts;
  
  protected volatile Map<TypeToken<? extends TimeSeriesDataType>, Tsdb1xPartialTimeSeries> last_pts;
  
  /**
   * Default ctor.
   * @param owner A non-null owner with configuration and reporting.
   * @param scanner A non-null HBase scanner to work with.
   * @param idx A zero based index when multiple salt scanners are in
   * use.
   * @throws IllegalArgumentException if the owner or scanner was null.
   */
  public Tsdb1xScanner2(final Tsdb1xScanners2 owner, 
                       final Scanner scanner, 
                       final int idx,
                       final RollupInterval rollup_interval) {
    if (owner == null) {
      throw new IllegalArgumentException("Owner cannot be null.");
    }
    if (scanner == null) {
      throw new IllegalArgumentException("Scanner cannot be null.");
    }
    this.owner = owner;
    this.scanner = scanner;
    this.idx = idx;
    this.rollup_interval = rollup_interval;
    state = State.CONTINUE;
    base_ts = new SecondTimeStamp(-1);
    last_ts = new SecondTimeStamp(-1);
    
    if (owner.filterDuringScan()) {
      keys_to_ids = new TLongObjectHashMap<ResolvingId>();
      skips = new TLongHashSet();
      keepers = new TLongHashSet();
    }
    last_pts = Maps.newHashMap();
  }
  
  /**
   * Called by the {@link Tsdb1xScanners} to initiate the next fetch of
   * data from the buffer and/or scanner.
   * 
   * @param result A non-null result set to decode the columns we find.
   * @param span An optional tracing span.
   */
  public void fetchNext(final Tsdb1xQueryResult ignored, final Span span) {
    if (owner.hasException()) {
      scanner.close();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Closing scanner due to upstream result exception.");
      }
      state = State.COMPLETE;
      owner.scannerDone();
      return;
    }
    
    // try for some data from HBase
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".fetchNext_" + idx)
          .start();
    } else {
      child = span;
    }
    
    scanner.nextRows()
      .addCallbacks(new ScannerCB(child), new ErrorCB(child));
  }
  
  /**
   * A callback attached to the scanner's {@link Scanner#nextRows()} call
   * that processes the rows returned.
   */
  final class ScannerCB implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
    
    /** A tracing span. */
    private final Span span;
    
    /** A counter for the total number of rows scanned in this pass/segment. */
    private long rows_scanned = 0;
    
    /**
     * Default ctor.
     * @param result The non-null result.
     * @param span An optional tracing span.
     */
    ScannerCB(final Span span) {
      if (span != null && span.isDebug()) {
        this.span = span.newChild(getClass().getName() + "_" + idx)
            .start();
      } else {
        this.span = span;
      }
    }

    @Override
    public Object call(final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
      if (rows == null) {
        complete(null, 0);
        return null;
      }
      
      if (owner.hasException()) {
        // bail out!
        complete(null, rows.size());
        return null;
      }
      
      final Span child;
      if (span != null) {
        child = span.newChild(getClass().getName() + "call_" + idx)
            .start();
      } else {
        child = null;
      }
      
      try {
        rows_scanned += rows.size();
        if (owner.filterDuringScan()) {
          final List<Deferred<Object>> deferreds = 
              Lists.newArrayListWithCapacity(rows.size());
          boolean keep_going = true;
          for (int i = 0; i < rows.size(); i++) {
            final ArrayList<KeyValue> row = rows.get(i);
            if (row.isEmpty()) {
              // should never happen
              if (LOG.isDebugEnabled()) {
                LOG.debug("Received an empty row from result set: " + rows);
              }
              continue;
            }
            
            final byte[] tsuid = owner.node().schema().getTSUID(row.get(0).key());
            deferreds.add(resolveAndFilter(tsuid, row, null, child));
          }
          
          return Deferred.groupInOrder(deferreds)
              .addCallbacks(new GroupResolutionCB(keep_going, child), 
                  new ErrorCB(child));
        } else {
          // load all
          for (int i = 0; i < rows.size(); i++) {
            final ArrayList<KeyValue> row = rows.get(i);
            if (row.isEmpty()) {
              // should never happen
              if (LOG.isDebugEnabled()) {
                LOG.debug("Received an empty row from result set: " + rows);
              }
              continue;
            }
            
            processRow(row, null /* TODO */);
            // TODO size of query
          }
        }
        
        if (owner.hasException()) {
          complete(child, rows.size());
        } else {
          return scanner.nextRows().addCallbacks(this, new ErrorCB(span));
        }
      } catch (Exception e) {
        LOG.error("Unexpected exception", e);
        complete(e, child, rows.size());
      }
      
      return null;
    }
    
    /**
     * Marks the scanner as complete, closing it and reporting to the owner
     * @param child An optional tracing span.
     * @param rows The number of rows found in this result set.
     */
    void complete(final Span child, final int rows) {
      complete(null, child, rows);
    }
    
    /**
     * Marks the scanner as complete, closing it and reporting to the owner
     * @param e An exception, may be null. If not null, calls 
     * {@link Tsdb1xScanners#exception(Throwable)}
     * @param child An optional tracing span.
     * @param rows The number of rows found in this result set.
     */
    void complete(final Exception e, final Span child, final int rows) {
      try {
      System.out.println("--------- COMPLETING ------------");
      // this will let the flush complete the last set.
      base_ts.updateEpoch(0L);
      
      if (last_pts.isEmpty()) {
        // never had any data so for the parent, mark everything as complete 
        // for this salt
        System.out.println("          FILLING ALL as we didn't find anything.");
        for (final Tsdb1xPartialTimeSeriesSet set : owner.sets.valueCollection()) {
          set.setCompleteAndEmpty();
        }
      } else {
        flushPartials();
        
        if (last_ts.compare(Op.NE, owner.end_ts)) {
          System.out.println("FILLING ...................");
          // We need to fill the end of the period
          // rollups
          last_ts.add(Duration.ofSeconds(3600));
          while (last_ts.compare(Op.LT, owner.end_ts)) {
            owner.getSet(last_ts).setCompleteAndEmpty();
            last_ts.add(Duration.ofSeconds(3600));
          }
        }
      }
      
      if (e != null) {
        if (child != null) {
          child.setErrorTags(e)
               .finish();
        }
        if (span != null) {
          span.setErrorTags(e)
              .finish();
        }
        state = State.EXCEPTION;
        owner.exception(e);
      } else {
        if (child != null) {
          child.setSuccessTags()
               .setTag("rows", rows)
               .setTag("buffered", 0 /*row_buffer == null ? 0 : row_buffer.size()*/)
               .finish();
        }
        if (span != null) {
          span.setSuccessTags()
              .setTag("totalRows", rows_scanned)
              .setTag("buffered", 0 /*row_buffer == null ? 0 : row_buffer.size()*/)
              .finish();
        }
        state = State.COMPLETE;
        owner.scannerDone();
      }
      scanner.close(); // TODO - attach a callback for logging in case
      clear();
      } catch (Throwable t) {
        LOG.error("WTF?", t);
      }
    }
    
    /** Called when the filter resolution is complete. */
    class GroupResolutionCB implements Callback<Object, ArrayList<Object>> {
      final boolean keep_going;
      final Span child;
      
      GroupResolutionCB(final boolean keep_going, final Span span) {
        this.keep_going = keep_going;
        this.child = span;
      }
      
      @Override
      public Object call(final ArrayList<Object> ignored) throws Exception {
        synchronized (keys_to_ids) {
          keys_to_ids.clear();
        }
        return scanner.nextRows().addCallbacks(ScannerCB.this, new ErrorCB(span));
      }
    }
  }
  
  /**
   * WARNING: Only call this single threaded!
   * @param row
   */
  private void processRow(final ArrayList<KeyValue> row, final RollupInterval rollup_interval) {
    owner.node().schema().baseTimestamp(row.get(0).key(), base_ts);
    if (base_ts.compare(Op.NE, last_ts)) {
      if (last_ts.epoch() == -1) {
        // we found the first value. So if we don't match the first 
        // set then we need to fill
        if (base_ts.compare(Op.NE, owner.start_ts)) {
          TimeStamp ts = owner.start_ts.getCopy();
          while (ts.compare(Op.LT, base_ts)) {
            owner.getSet(ts).setCompleteAndEmpty();
            // TODO - rolluups
            ts.add(Duration.ofSeconds(3600));
          }
        }
      } else {
        TimeStamp ts = last_ts.getCopy();
        ts.add(Duration.ofSeconds(3600)); // TODO  -rollup
        if (ts.compare(Op.NE, base_ts)) {
          // FILL
          System.out.println("       ^^^^^^^^^^ Filling middle");
          while (ts.compare(Op.LT, base_ts)) {
            owner.getSet(ts).setCompleteAndEmpty();
            ts.add(Duration.ofSeconds(3600));
          }
        }
      }
      
      // flush em!
      flushPartials();
    }
    last_ts.update(base_ts);
    
    final long hash = LongHashFunction.xx_r39().hashBytes(
        owner.node().schema().getTSUID(row.get(0).key()));

    // TODO - find a better spot
    synchronized (owner.ts_ids) {
      if (!owner.ts_ids.containsKey(hash)) {
        owner.ts_ids.put(hash, new TSUID(owner.node().schema().getTSUID(row.get(0).key()), owner.node().schema()));
      }
    }
    
    Tsdb1xPartialTimeSeries pts = last_pts.get(NumericLongArrayType.TYPE);
    for (final KeyValue column : row) {
      if (rollup_interval == null && (column.qualifier().length & 1) == 0) {
        // it's a NumericDataType
        if (!owner.node().fetchDataType(NUMERIC_TYPE)) {
          // filter doesn't want #'s
          // TODO - dropped counters
          continue;
        }
        
        if (pts == null) {
          pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
          last_pts.put(NumericLongArrayType.TYPE, pts);
        } else if (pts.getType() != NumericLongArrayType.TYPE) {
          pts = last_pts.get(NumericLongArrayType.TYPE);
          if (pts == null) {
            pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
            last_pts.put(NumericLongArrayType.TYPE, pts);
          }
        }
        
        if (!pts.sameHash(hash)) {
          System.out.println("          gimme new hash!");
          flushPartials();
          pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
          last_pts.put(NumericLongArrayType.TYPE, pts);
        }
        System.out.println("          HASH: " + hash  + "  PTS: " + pts.idHash());
        pts.addColumn((byte) 0, 
                      base_ts,
                      column.qualifier(), 
                      column.value(),
                      owner.node().schema().arrayPool(),
                      hash,
                      owner.getSet(base_ts));
      } else if (rollup_interval == null) {
        final byte prefix = column.qualifier()[0];
        
        if (prefix == Schema.APPENDS_PREFIX) {
          if (!owner.node().fetchDataType((byte) 1)) {
            // filter doesn't want #'s
            continue;
          } else {
            if (pts == null) {
              pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
              last_pts.put(NumericLongArrayType.TYPE, pts);
            } else if (pts.getType() != NumericLongArrayType.TYPE) {
              pts = last_pts.get(NumericLongArrayType.TYPE);
              if (pts == null) {
                pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
                last_pts.put(NumericLongArrayType.TYPE, pts);
              }
            }

            if (!pts.sameHash(hash)) {
              System.out.println("          gimme new hash!");
              flushPartials();
              pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE);
              last_pts.put(NumericLongArrayType.TYPE, pts);
            }
            System.out.println("          HASH: " + hash  + "  PTS: " + pts.idHash());
            pts.addColumn(Schema.APPENDS_PREFIX, 
                          base_ts,
                          column.qualifier(), 
                          column.value(),
                          owner.node().schema().arrayPool(),
                          hash,
                          owner.getSet(base_ts));
          }
        } else if (owner.node().fetchDataType(prefix)) {
          // TODO - find the right type
        } else {
          // TODO - log drop
        }
      } else {
        // TODO - rollup
        // Only numerics are rolled up right now. And we shouldn't have
        // a rollup query if the user doesn't want rolled-up data.
        //numerics.addColumn(NUMERIC_PREFIX, kv.qualifier(), kv.value());
      }
    }
  }
  
  /** @return The state of this scanner. */
  State state() {
    return state;
  }
  
  /** Closes the scanner. */
  void close() {
    try {
      scanner.close();
    } catch (Exception e) {
      LOG.error("Failed to close scanner", e);
    }
    clear();
  }
  
  /** Clears the filter map and sets when the scanner is done so GC can
   * clean up quicker. */
  private void clear() {
    if (keys_to_ids != null) {
      synchronized (keys_to_ids) {
        keys_to_ids.clear();
      }
    }
    if (skips != null) {
      synchronized (skips) {
        skips.clear();
      }
    }
    if (keepers != null) {
      synchronized (keepers) {
        keepers.clear();
      }
    }
  }
  
  /** The error back used to catch all kinds of exceptions. Closes out 
   * everything after passing the exception to the owner. */
  final class ErrorCB implements Callback<Object, Exception> {
    final Span span;
    
    ErrorCB(final Span span) {
      this.span = span;
    }
    
    @Override
    public Object call(final Exception ex) throws Exception {
      LOG.error("Unexpected exception", 
          (ex instanceof DeferredGroupException ? 
              Exceptions.getCause((DeferredGroupException) ex) : ex));
      state = State.EXCEPTION;
      owner.exception((ex instanceof DeferredGroupException ? 
          Exceptions.getCause((DeferredGroupException) ex) : ex));
      scanner.close();
      clear();
      return null;
    }
  }
  
  void flushPartials() {
    try {
      System.out.println("----- FLUSH PARTIALS -----");
      Iterator<Tsdb1xPartialTimeSeries> iterator = last_pts.values().iterator();
      while (iterator.hasNext()) {
        Tsdb1xPartialTimeSeries series = iterator.next();
        if (!iterator.hasNext() && series.set().start().compare(Op.NE, base_ts)) {
          ((Tsdb1xPartialTimeSeriesSet) series.set()).increment(series, true);
        } else {
          ((Tsdb1xPartialTimeSeriesSet) series.set()).increment(series, false);
        }
        iterator.remove();
        System.out.println("       REMOVED: " + System.identityHashCode(series));
      }
    } catch (Throwable t) {
      LOG.error("WTF?", t);
    }
  }
  
  /**
   * Evaluates a row against the skips, keepers and may resolve it if
   * necessary when we have to go through filters that couldn't be sent
   * to HBase.
   * 
   * @param tsuid A non-null TSUID.
   * @param row A non-null row to process.
   * @param result A non-null result to store successful matches into.
   * @param span An optional tracing span.
   * @return A deferred to wait on before starting the next fetch.
   */
  final Deferred<Object> resolveAndFilter(final byte[] tsuid, 
                                          final ArrayList<KeyValue> row, 
                                          final Tsdb1xQueryResult result, 
                                          final Span span) {
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);
    synchronized (skips) {
      if (skips.contains(hash)) {
        // discard
        // TODO - counters
        return Deferred.fromResult(null);
      }
    }
    
    synchronized (keepers) {
      if (keepers.contains(hash)) {
        processRow(row, rollup_interval);
        //result.decode(row, rollup_interval);
        return Deferred.fromResult(null);
      }
    }
    
    synchronized (keys_to_ids) {
      ResolvingId id = keys_to_ids.get(hash);
      if (id == null) {
        ResolvingId new_id = new ResolvingId(tsuid, hash);
        final ResolvingId extant = keys_to_ids.putIfAbsent(hash, new_id);
        if (extant == null) {
          // start resolution of the tags to strings, then filter
          return new_id.decode(span)
              .addCallback(new ResolvedCB(row, result));
        } else {
          // add it
          return extant.deferred.addCallback(new ResolvedCB(row, result));
        }
      } else {
        return id.deferred.addCallback(new ResolvedCB(row, result));
      }
    }
  }
  
  /** Simple class for rows waiting on resolution. */
  class ResolvedCB implements Callback<Object, Boolean> {
    private final ArrayList<KeyValue> row;
    private final Tsdb1xQueryResult result;
    
    ResolvedCB(final ArrayList<KeyValue> row, final Tsdb1xQueryResult result) {
      this.row = row;
      this.result = result;
    }
    
    @Override
    public Object call(final Boolean matched) throws Exception {
      if (matched != null && matched) {
        try {
        synchronized (Tsdb1xScanner2.this) {
          System.out.println("                WORKING: " + row + "  M: " + matched);
          processRow(row, null /* TODO */);
        }
        //result.decode(row, rollup_interval);
        }catch (Throwable t) {
          LOG.error("WTF?", t);
        }
      }
      return null;
    }
    
  }
  
  /**
   * An override of the {@link TSUID} class that holds a reference to the
   * resolution deferred so others rows with different timestamps but the
   * same TSUID can wait for a single result to be resolved.
   * <p>
   * <b>NOTE:</b> Do not call {@link TSUID#decode(boolean, Span)} directly!
   * Instead call {@link ResolvingId#decode(Span)}.
   * <p>
   * <b>NOTE:</b> If skip_nsui was set to true, this will return a false
   * for any rows that didn't resolve properly. If set to true, then this
   * will return a {@link NoSuchUniqueId} exception.
   */
  private class ResolvingId extends TSUID implements Callback<Void, TimeSeriesStringId> {
    /** The computed hash of the TSUID. */
    private final long hash;
    
    /** The resolution deferred for others to wait on. */
    private Deferred<Boolean> deferred;
    
    /** A child tracing span. */
    private Span child;
    
    /**
     * Default ctor.
     * @param tsuid A non-null TSUID.
     * @param hash The computed hash of the TSUID.
     */
    public ResolvingId(final byte[] tsuid, final long hash) {
      super(tsuid, owner.node().schema());
      this.hash = hash;
      deferred = new Deferred<Boolean>();
    }

    /**
     * Starts decoding the TSUID into a string and returns the deferred 
     * for other TSUIDs to wait on.
     * 
     * @param span An optional tracing span.
     * @return A deferred resolving to true if the TSUID passed all of
     * the scan filters, false if not. Or an exception if something went
     * pear shaped.
     */
    Deferred<Boolean> decode(final Span span) {
      if (span != null && span.isDebug()) {
        child = span.newChild(getClass().getName() + "_" + idx)
            .start();
      } else {
        child = span;
      }
      decode(false, child)
          .addCallback(this)
          .addErrback(new ErrorCB(null));
      return deferred;
    }
    
    @Override
    public Void call(final TimeSeriesStringId id) throws Exception {
      final Span grand_child;
      if (child != null && child.isDebug()) {
        grand_child = child.newChild(getClass().getName() + ".call_" + idx)
            .start();
      } else {
        grand_child = child;
      }
      
      QueryFilter filter = ((TimeSeriesDataSourceConfig) 
          owner.node().config()).getFilter();
      if (filter == null) {
        filter = owner.node().pipelineContext().query().getFilter(
            ((TimeSeriesDataSourceConfig) owner.node().config()).getFilterId());
      }
      if (filter == null) {
        final IllegalStateException ex = new IllegalStateException(
            "No filter found when resolving filter IDs?");
        if (child != null) {
          child.setErrorTags(ex)
               .finish();
        }
        deferred.callback(ex);
      }
      
      if (FilterUtils.matchesTags(filter, id.tags(), null)) {
        synchronized (keepers) {
          keepers.add(hash);
        }
        if (grand_child != null) {
          grand_child.setSuccessTags()
                     .setTag("resolved", "true")
                     .setTag("matched", "true")
                     .finish();
        }
        if (child != null) {
          child.setSuccessTags()
               .setTag("resolved", "true")
               .setTag("matched", "true")
               .finish();
        }
        deferred.callback(true);
      } else {
        synchronized (skips) {
          skips.add(hash);
        }
        if (grand_child != null) {
          grand_child.setSuccessTags()
                     .setTag("resolved", "true")
                     .setTag("matched", "false")
                     .finish();
        }
        if (child != null) {
          child.setSuccessTags()
               .setTag("resolved", "true")
               .setTag("matched", "false")
               .finish();
        }
        deferred.callback(false);
      }
      return null;
    }
    
    class ErrorCB implements Callback<Void, Exception> {
      final Span grand_child;
      
      ErrorCB(final Span grand_child) {
        this.grand_child = grand_child;
      }
      
      @Override
      public Void call(final Exception ex) throws Exception {
        if (ex instanceof NoSuchUniqueId && owner.node().skipNSUI()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Row contained a bad UID: " + Bytes.pretty(tsuid) 
              + " " + ex.getMessage());
          }
          synchronized (skips) {
            skips.add(hash);
          }
          if (grand_child != null) {
            grand_child.setSuccessTags()
                       .setTag("resolved", "false")
                       .finish();
          }
          if (child != null) {
            child.setSuccessTags()
                 .setTag("resolved", "false")
                 .finish();
          }
          deferred.callback(false);
          return null;
        }
        if (grand_child != null) {
          grand_child.setErrorTags(ex)
                     .setTag("resolved", "false")
                     .finish();
        }
        if (child != null) {
          child.setErrorTags(ex)
               .setTag("resolved", "false")
               .finish();
        }
        deferred.callback(ex);
        return null;
      }
    }
  }
}

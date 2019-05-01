// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericByteArraySummaryType;
import net.opentsdb.data.types.numeric.NumericLongArrayType;
import net.opentsdb.pools.ByteArrayPool;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.HBaseExecutor.State;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.TSUID;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeries;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Exceptions;

/**
 * 
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
  
  protected Map<TypeToken<? extends TimeSeriesDataType>, 
    Tsdb1xPartialTimeSeries> last_pts;
  
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
   * Called by the {@link Tsdb1xScanners2} to initiate the next fetch of
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
          final List<Deferred<ArrayList<KeyValue>>> deferreds = 
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
            
            processRow(row);
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
      // this will let the flush complete the last set.
      base_ts.updateEpoch(0L);
      
      if (last_pts.isEmpty()) {
        // never had any data so for the parent, mark everything as complete 
        // for this salt
        for (final Tsdb1xPartialTimeSeriesSet set : owner.sets.get(owner.scanner_index).valueCollection()) {
          set.setCompleteAndEmpty(owner.scanner_index + 1 == owner.scanners.size());
        }
      } else {
        flushPartials();
        
        if (last_ts.compare(Op.NE, owner.timestamps.get(owner.scanner_index).getValue())) {
          // We need to fill the end of the period
          // rollups
          last_ts.add(owner.durations.get(owner.scanner_index));
          while (last_ts.compare(Op.LT, owner.timestamps.get(owner.scanner_index).getValue())) {
            owner.getSet(last_ts).setCompleteAndEmpty(owner.scanner_index + 1 == owner.scanners.size());
            last_ts.add(owner.durations.get(owner.scanner_index));
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
    class GroupResolutionCB implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
      final boolean keep_going;
      final Span child;
      
      GroupResolutionCB(final boolean keep_going, final Span span) {
        this.keep_going = keep_going;
        this.child = span;
      }
      
      @Override
      public Object call(final ArrayList<ArrayList<KeyValue>> rows) throws Exception {
        for (final ArrayList<KeyValue> row : rows) {
          if (row != null) {
            processRow(row);
          }
        }
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
  private void processRow(final ArrayList<KeyValue> row) {
    try {
    owner.node().schema().baseTimestamp(row.get(0).key(), base_ts);
    if (base_ts.compare(Op.NE, last_ts)) {
      if (last_ts.epoch() == -1) {
        // we found the first value. So if we don't match the first 
        // set then we need to fill
        if (base_ts.compare(Op.NE, owner.timestamps.get(owner.scanner_index).getKey())) {
          TimeStamp ts = owner.timestamps.get(owner.scanner_index).getKey().getCopy();
          while (ts.compare(Op.LT, base_ts)) {
            owner.getSet(ts).setCompleteAndEmpty(owner.scanner_index + 1 == owner.scanners.size());
            ts.add(owner.durations.get(owner.scanner_index));
          }
        }
      } else {
        TimeStamp ts = last_ts.getCopy();
        ts.add(owner.durations.get(owner.scanner_index));
        if (ts.compare(Op.NE, base_ts)) {
          // FILL
          while (ts.compare(Op.LT, base_ts)) {
            owner.getSet(ts).setCompleteAndEmpty(owner.scanner_index + 1 == owner.scanners.size());
            ts.add(owner.durations.get(owner.scanner_index));
          }
        }
      }
      
      // flush em!
      flushPartials();
    }
    last_ts.update(base_ts);
    
    final byte[] tsuid = owner.node().schema().getTSUID(row.get(0).key());
    final long hash = LongHashFunction.xx_r39().hashBytes(tsuid);

    // TODO - find a better spot. We may not pull any data from this row so we
    // shouldn't bother putting it in the ids.
    synchronized (owner.ts_ids) {
      if (!owner.ts_ids.containsKey(hash)) {
        owner.ts_ids.put(hash, new TSUID(tsuid, owner.node().schema()));
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
          pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
          last_pts.put(NumericLongArrayType.TYPE, pts);
        } else if (pts.value().type() != NumericLongArrayType.TYPE) {
          pts = last_pts.get(NumericLongArrayType.TYPE);
          if (pts == null) {
            pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
            last_pts.put(NumericLongArrayType.TYPE, pts);
          }
        }
        
        if (!pts.sameHash(hash)) {
          flushPartials();
          pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
          last_pts.put(NumericLongArrayType.TYPE, pts);
        }
        
        pts.addColumn((byte) 0,
                      column.qualifier(), 
                      column.value());
      } else if (rollup_interval == null) {
        final byte prefix = column.qualifier()[0];
        
        if (prefix == Schema.APPENDS_PREFIX) {
          if (!owner.node().fetchDataType((byte) 1)) {
            // filter doesn't want #'s
            continue;
          } else {
            if (pts == null) {
              pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
              last_pts.put(NumericLongArrayType.TYPE, pts);
            } else if (pts.value().type() != NumericLongArrayType.TYPE) {
              pts = last_pts.get(NumericLongArrayType.TYPE);
              if (pts == null) {
                pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
                last_pts.put(NumericLongArrayType.TYPE, pts);
              }
            }

            if (!pts.sameHash(hash)) {
              flushPartials();
              pts = owner.node().schema().newSeries(NumericLongArrayType.TYPE, base_ts, hash, owner.node().schema().arrayPool(), owner.getSet(base_ts), rollup_interval);
              last_pts.put(NumericLongArrayType.TYPE, pts);
            }
            
            pts.addColumn(Schema.APPENDS_PREFIX, 
                          column.qualifier(), 
                          column.value());
          }
        } else if (owner.node().fetchDataType(prefix)) {
          // TODO - find the right type
        } else {
          // TODO - log drop
        }
      } else {
        // Only numerics are rolled up right now. And we shouldn't have
        // a rollup query if the user doesn't want rolled-up data.
        pts = last_pts.get(NumericByteArraySummaryType.TYPE);
        if (pts == null) {
          pts = owner.node().schema().newSeries(NumericByteArraySummaryType.TYPE, base_ts, hash, 
              owner.node().pipelineContext().tsdb().getRegistry().getObjectPool(ByteArrayPool.TYPE), owner.getSet(base_ts), rollup_interval);
          last_pts.put(NumericByteArraySummaryType.TYPE, pts);
        } else if (pts.value().type() != NumericByteArraySummaryType.TYPE) {
          pts = last_pts.get(NumericByteArraySummaryType.TYPE);
          if (pts == null) {
            pts = owner.node().schema().newSeries(NumericByteArraySummaryType.TYPE, base_ts, hash, 
                owner.node().pipelineContext().tsdb().getRegistry().getObjectPool(ByteArrayPool.TYPE), owner.getSet(base_ts), rollup_interval);
            last_pts.put(NumericByteArraySummaryType.TYPE, pts);
          }
        }

        if (!pts.sameHash(hash)) {
          flushPartials();
          pts = owner.node().schema().newSeries(NumericByteArraySummaryType.TYPE, base_ts, hash, 
              owner.node().pipelineContext().tsdb().getRegistry().getObjectPool(ByteArrayPool.TYPE), owner.getSet(base_ts), rollup_interval);
          last_pts.put(NumericByteArraySummaryType.TYPE, pts);
        }
        
        pts.addColumn(Schema.APPENDS_PREFIX, 
                      column.qualifier(), 
                      column.value());
      }
    }
    } catch (Throwable t) {
      LOG.error("WTF?", t);
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
      Iterator<Tsdb1xPartialTimeSeries> iterator = last_pts.values().iterator();
      while (iterator.hasNext()) {
        Tsdb1xPartialTimeSeries series = iterator.next();
        if (!iterator.hasNext() && series.set().start().compare(Op.NE, base_ts)) {
          ((Tsdb1xPartialTimeSeriesSet) series.set()).increment(series, true);
        } else {
          ((Tsdb1xPartialTimeSeriesSet) series.set()).increment(series, false);
        }
        iterator.remove();
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
  final Deferred<ArrayList<KeyValue>> resolveAndFilter(final byte[] tsuid, 
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
        processRow(row);
        return Deferred.fromResult(row);
      }
    }
    
    synchronized (keys_to_ids) {
      ResolvingId id = keys_to_ids.get(hash);
      Deferred<Boolean> d = new Deferred<Boolean>();
      if (id == null) {
        ResolvingId new_id = new ResolvingId(tsuid, hash);
        final ResolvingId extant = keys_to_ids.putIfAbsent(hash, new_id);
        if (extant == null) {
          // start resolution of the tags to strings, then filter
          new_id.decode(span);
          new_id.addCallback(d);
          return d.addCallback(new ResolvedCB(row));
        } else {
          // add it
          extant.addCallback(d);
          return d.addCallback(new ResolvedCB(row));
        }
      } else {
        id.addCallback(d);
        return d.addCallback(new ResolvedCB(row));
      }
    }
  }
  
  /** Simple class for rows waiting on resolution. */
  class ResolvedCB implements Callback<ArrayList<KeyValue>, Boolean> {
    private final ArrayList<KeyValue> row;
    
    ResolvedCB(final ArrayList<KeyValue> row) {
      this.row = row;
    }
    
    @Override
    public ArrayList<KeyValue> call(final Boolean matched) throws Exception {
      if (matched != null && (Boolean) matched) {
        return row;
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
    
    private boolean complete;
    private Object result;
    private List<Deferred<Boolean>> deferreds;
    
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
      deferreds = Lists.newArrayList();
    }

    synchronized void addCallback(final Deferred<Boolean> cb) {
      if (complete) {
        try {
          cb.callback(result);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else {
        deferreds.add(cb);
      }
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
    void decode(final Span span) {
      if (span != null && span.isDebug()) {
        child = span.newChild(getClass().getName() + "_" + idx)
            .start();
      } else {
        child = span;
      }
      decode(false, child)
          .addCallback(this)
          .addErrback(new ErrorCB(null));
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
        synchronized (ResolvingId.this) {
          result = ex;
          complete = true;
        }
        flush();
        return null;
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
        synchronized (ResolvingId.this) {
          result = true;
          complete = true;
        }
        flush();
        return null;
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
        synchronized (ResolvingId.this) {
          result = false;
          complete = true;
        }
        flush();
        return null;
      }
    }
    
    void flush() {
      for (final Deferred<Boolean> cb : deferreds) {
        try {
          cb.callback(result);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
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
          synchronized (ResolvingId.this) {
            result = false;
            complete = true;
          }
          flush();
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
        synchronized (ResolvingId.this) {
          result = ex;
          complete = true;
        }
        flush();
        return null;
      }
    }
  }
}

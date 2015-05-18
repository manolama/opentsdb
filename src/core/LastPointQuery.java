// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.meta.TSUIDQuery;
import net.opentsdb.uid.UniqueId;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

/**
 * Processes one or more queries for the latest data point. It can be a mix of
 * TSUID and metric queries.
 * @since 2.1
 */
public class LastPointQuery {
  private static final Logger LOG = LoggerFactory.getLogger(LastPointQuery.class);
  
  /** Whether or not to resolve the metric and tag results to strings */
  private boolean resolve_names;
  
  /** How many hours in the past to scan for. Bypasses the meta table */
  private int back_scan;
  
  /** A list of one or more sub queries */
  private List<LastPointSubQuery> sub_queries;
  
  /**
   * Default Constructor necessary for de/serialization
   */
  public LastPointQuery() {
    
  }
  
  /**
   * Executes the queries and compiles the results into a list of 
   * IncomingDataPoints.
   * @param tsdb The TSDB to use for communicating with storage and fetching 
   * settings
   * @return A deferred with either valid results, an empty list or an exception
   */
  public Deferred<ArrayList<IncomingDataPoint>> runQueries(final TSDB tsdb) {
    if (sub_queries == null || sub_queries.isEmpty()) {
      throw new IllegalArgumentException("Must have one or more sub queries");
    }
    
    final Deferred<ArrayList<IncomingDataPoint>> deferred = 
        new Deferred<ArrayList<IncomingDataPoint>>();
    final boolean meta_enabled = tsdb.getConfig().enable_tsuid_incrementing();
    
    if (!meta_enabled) {
      // override with the default if the user hasn't requested a specific 
      // value. This forces the TSUID queries but for the metrics we have to
      // finagle it differently.
      if (back_scan < 1) {
        back_scan = 
            tsdb.getConfig().getInt("tsd.query.lastpoint.back_scan_hours");
      }
    }
    
    // list of getLastPoint calls
    final List<Deferred<IncomingDataPoint>> calls = 
      new ArrayList<Deferred<IncomingDataPoint>>();
    // list of calls to TSUIDQuery for scanning the tsdb-meta table
    final List<Deferred<Object>> tsuid_query_wait = 
      new ArrayList<Deferred<Object>>();
        
    /**
     * Called after scanning the tsdb-meta table for TSUIDs that match the given
     * metric and/or tags. If matches were found, it fires off a number of
     * getLastPoint requests, adding the deferreds to the calls list
     */
    final class TSUIDQueryCB implements Callback<Object, ByteMap<Long>> {
      public Object call(final ByteMap<Long> tsuids) throws Exception {
        if (tsuids == null || tsuids.isEmpty()) {
          return null;
        }
        
        for (Map.Entry<byte[], Long> entry : tsuids.entrySet()) {
          calls.add(TSUIDQuery.getLastPoint(tsdb, entry.getKey(), resolve_names, 
              back_scan, entry.getValue()));
        }
        return null;
      }
    }
    
    /**
     * Used to wait on the list of data point deferreds. Once they're all done
     * this will return the results to the call via the serializer
     */
    final class FinalCB implements Callback<Object, ArrayList<IncomingDataPoint>> {
      public Object call(final ArrayList<IncomingDataPoint> data_points) 
        throws Exception {
        // kick out nulls
        final List<IncomingDataPoint> dps = 
            new ArrayList<IncomingDataPoint>(data_points.size());
        for (final IncomingDataPoint dp : data_points) {
          if (dp != null) {
            dps.add(dp);
          }
        }
        deferred.callback(dps);
        return null;
      }
    }
    
    /**
     * Callback used to force the thread to wait for the TSUIDQueries to complete
     */
    final class TSUIDQueryWaitCB implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> complete) throws Exception {
        return Deferred.group(calls)
            .addCallback(new FinalCB())
            .addErrback(new ErrBack(deferred));
      }
    }

    // start executing the queries
    for (LastPointSubQuery sub_query : sub_queries) {
      // TSUID queries take precedence so if there are any TSUIDs listed, 
      // process the TSUIDs and ignore the metric/tags
      if (sub_query.getTSUIDs() != null && !sub_query.getTSUIDs().isEmpty()) {
        for (String tsuid : sub_query.getTSUIDs()) {
          calls.add(TSUIDQuery.getLastPoint(tsdb, UniqueId.stringToUid(tsuid), 
              resolve_names, back_scan, 0));
        }
      } else {
        final TSUIDQuery tsuid_query = new TSUIDQuery(tsdb);
        @SuppressWarnings("unchecked")
        final HashMap<String, String> tags = 
          (HashMap<String, String>) (sub_query.getTags() != null ? 
            sub_query.getTags() : Collections.EMPTY_MAP);
        tsuid_query.setQuery(sub_query.getMetric(), tags);
        tsuid_query_wait.add(
            tsuid_query.getLastWriteTimes().addCallback(new TSUIDQueryCB()));
      }
    }
    
    if (!tsuid_query_wait.isEmpty()) {
      Deferred.group(tsuid_query_wait)
        .addCallback(new TSUIDQueryWaitCB())
        .addErrback(new ErrBack(deferred));
    } else {
      Deferred.group(calls)
        .addCallback(new FinalCB())
        .addErrback(new ErrBack(deferred));
    }
    
    return deferred;
  }

  /** @return Whether or not to resolve the UIDs to names */
  public boolean getResolveNames() {
    return resolve_names;
  }
  
  /** @return Number of hours to scan back in time looking for data */
  public int getBackScan() {
    return back_scan;
  }
  
  /** @return A list of sub queries */
  public List<LastPointSubQuery> getQueries() {
    return sub_queries;
  }
  
  /** @param resolve_names Whether or not to resolve the UIDs to names */
  public void setResolveNames(final boolean resolve_names) {
    this.resolve_names = resolve_names;
  }
  
  /** @param back_scan Number of hours to scan back in time looking for data */
  public void setBackScan(final int back_scan) {
    this.back_scan = back_scan;
  }
  
  /** @param queries A list of sub queries to execute */
  public void setQueries(final List<LastPointSubQuery> queries) {
    this.sub_queries = queries;
  }

  /**
   * Used to catch exceptions 
   */
  private final static class ErrBack implements Callback<Object, Exception> {
    final Deferred<ArrayList<IncomingDataPoint>> deferred;
    
    public ErrBack(final Deferred<ArrayList<IncomingDataPoint>> deferred) {
      this.deferred = deferred;
    }
    
    public Object call(final Exception e) throws Exception {
      Throwable ex = e;
      while (ex.getClass().equals(DeferredGroupException.class)) {
        if (ex.getCause() == null) {
          LOG.warn("Unable to get to the root cause of the DGE");
          break;
        }
        ex = ex.getCause();
      }
      if (ex != null) {
        deferred.callback(ex);
      } else {
        deferred.callback(e);
      }
      return null;
    }  
  }

}

// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.TimeSeries;
import net.opentsdb.formatters.Ascii;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;

/**
 * Used to be the GraphHandler, but we won't always be requesting graphs. Instead we'll
 * do the heavy lifting of parsing the query (the {@code /q} endpoint) here and
 * if the user requests it, we'll pass it on to the graph handler. Otherwise we'll just
 * return some JSON for the querent to play with
 */
public class QueryHandler implements HttpRpc {
  
  private static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

  /** 
   * Checks the cache first for valid data, then performs one or more queries against
   * HBase to fetch data, and stores data in the cache if applicable.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query to work with
   * @throws IOException 
   */
  public void execute(final TSDB tsdb, final HttpQuery query) throws IOException {
    final long start_time = query.getQueryStringDate("start");
    final boolean nocache = query.hasQueryStringParam("nocache");
    long end_time = query.getQueryStringDate("end");
    final int query_hash = query.getQueryStringHash();
    
    LOG.trace(String.format("HTTP Start [%d] End [%d]", start_time, end_time));
    // first, see if we can satisfy the request from cache
    if (!nocache && query.getCache().readCache(query_hash, query)){
      // satisfied from cache!!
      return;
    }
    
    // data checks
    if (start_time < 1) {
      throw BadRequestException.missingParameter("start");
    }
    final long now = System.currentTimeMillis() / 1000;
    if (end_time < 1) {
      end_time = now;
    }
    
    // get the cache directory
    String basepath = tsdb.getConfig().cacheDirectory();
    if (System.getProperty("os.name").contains("Windows")){
      if (!basepath.endsWith("\\"))
        basepath += "\\";
    }else{
      if (!basepath.endsWith("/"))
        basepath += "/";     
    }
    
    // append the hash of the query string so we have effective caching
    basepath += Integer.toHexString(query_hash);

    // determine how many HBase queries we'll need to run
    int total_queries = 0;
    Query[] tsdbqueries = parseQuery(tsdb, query);

    // loop through the queries and set the timestamps
    for (final Query tsdbquery : tsdbqueries) {
      try {
        tsdbquery.setStartTime(start_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("start time: " + e.getMessage());
      }
      try {
        tsdbquery.setEndTime(end_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("end time: " + e.getMessage());
      }
      total_queries++;
    }
    
    // setup the proper emitter object
//    DataEmitter emitter = null;
//    if (JSON_HTTP.getJsonRequested(query))
//      emitter = new JsonEmitter(start_time, end_time, query.getQueryString(), query_hash);
//    else if (query.hasQueryStringParam("ascii"))
//      emitter = new AsciiEmitter(start_time, end_time, query.getQueryString(), query_hash);
//    else
//     emitter = new GnuGraphEmitter(start_time, end_time, query.getQueryString(), query_hash);    
//    emitter.setBasepath(basepath);
    
    ArrayList<TimeSeries> timeseries = new ArrayList<TimeSeries>();
    final int nqueries = tsdbqueries.length;
    for (int i = 0; i < nqueries; i++) {
      try {  // execute the TSDB query!
        // XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
        // TODO(tsuna): Optimization: run each query in parallel.
        final DataPoints[] series = tsdbqueries[i].run();
        
        TimeSeries ts = new TimeSeries();
        // loop through the series and add them to the emitter
        for (final DataPoints datapoints : series) {
 
          // todo - hackish crap, copy to TimeSeries
          ts.metric_name = datapoints.metricName();
          ts.tags = datapoints.getTags();
          //data.metadata = ?
          ts.dps = new TreeMap<Long, Object>();
          for (final DataPoint d : datapoints){
            // if aggregator == none, strip out any dps that fall outside the requested timespan
            if (tsdbqueries[i].getAggregator().toString() == "none"){
              if (d.timestamp() < start_time || d.timestamp() > end_time)
                continue;
            }
            if (d.isInteger())
              ts.dps.put(d.timestamp(), d.longValue());
            else
              ts.dps.put(d.timestamp(), d.doubleValue());            
          }

          for (String ag : datapoints.getAggregatedTags()){
            System.out.println("Aggregated tag: " + ag);
          }
          // don't get metadata, it could add WAY too much data to the output when we really just want
          // the basics
          //ts.metadata = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(datapoints.getUID().get(0)));
          ts.uids = (ArrayList<String>) datapoints.getUID();
          ts.aggregator = tsdbqueries[i].getAggregator().toString();
          timeseries.add(ts);
          //emitter.add(datapoints);
        }
      } catch (RuntimeException e) {
        LOG.info("Query failed (stack trace coming): " + tsdbqueries[i]);
        throw e;
      }
      tsdbqueries[i] = null;  // free()
    }
    tsdbqueries = null;  // free()
    
    // process the emitter
//    if (!emitter.processData()){
//      LOG.error("Processing error: " + emitter.getError());
//      query.sendReply(emitter.getError());
//      return;
//    }

    // cache the response if told to
//    HttpCacheEntry entry = emitter.getCacheData();
//    if (!nocache && !query.getCache().storeCache(entry)){
//      LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
//    }
    
    // return data
//    if (entry.getFileOnly()){
//      query.sendFile(entry.getFile(), (int)entry.getExpire());
//    }else{
//      query.sendReply(entry.getData());
//    }
    
    // spit it out
    if (JSON_HTTP.getJsonRequested(query)){
      //emitter = new JsonEmitter(start_time, end_time, query.getQueryString(), query_hash);
      TsdbJSON formatter = new TsdbJSON();
      formatter.setTimeseries(timeseries);
      query.sendReply(formatter.getOutput());
    }else if (query.hasQueryStringParam("ascii")){
      Ascii formatter = new Ascii();
      formatter.setTimeseries(timeseries);
      query.sendReply(formatter.getOutput());
      //emitter = new AsciiEmitter(start_time, end_time, query.getQueryString(), query_hash);
    }else
      query.sendReply("Blarg!!!");
     //emitter = new GnuGraphEmitter(start_time, end_time, query.getQueryString(), query_hash);    
    //emitter.setBasepath(basepath);
    
  }
  
  /**
   * Parses the {@code /q} query in a list of {@link Query} objects.
   * @param tsdb The TSDB to use.
   * @param query The HTTP query for {@code /q}.
   * @return The corresponding {@link Query} objects.
   * @throws BadRequestException if the query was malformed.
   * @throws IllegalArgumentException if the metric or tags were malformed.
   */
  private static Query[] parseQuery(final TSDB tsdb, final HttpQuery query) {
    final List<String> ms = query.getQueryStringParams("m");
    if (ms == null) {
      throw BadRequestException.missingParameter("m");
    }
    final Query[] tsdbqueries = new Query[ms.size()];
    int nqueries = 0;
    for (final String m : ms) {
      // m is of the following forms:
      //   agg:[interval-agg:][rate:]metric[{tag=value,...}]
      // Where the parts in square brackets `[' .. `]' are optional.
      final String[] parts = Tags.splitString(m, ':');
      int i = parts.length;
      if (i < 2 || i > 4) {
        throw new BadRequestException("Invalid parameter m=" + m + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
      }
      final Aggregator agg = getAggregator(parts[0]);
      i--;  // Move to the last part (the metric name).
      final HashMap<String, String> parsedtags = new HashMap<String, String>();
      final String metric = Tags.parseWithMetric(parts[i], parsedtags);
      final boolean rate = "rate".equals(parts[--i]);
      if (rate) {
        i--;  // Move to the next part.
      }
      final Query tsdbquery = tsdb.newQuery();
      try {
        tsdbquery.setTimeSeries(metric, parsedtags, agg, rate);
      } catch (NoSuchUniqueName e) {
        throw new BadRequestException(e.getMessage());
      }
      // downsampling function & interval.
      if (i > 0) {
        final int dash = parts[1].indexOf('-', 1);  // 1st char can't be `-'.
        if (dash < 0) {
          throw new BadRequestException("Invalid downsampling specifier '"
                                        + parts[1] + "' in m=" + m);
        }
        Aggregator downsampler;
        try {
          downsampler = Aggregators.get(parts[1].substring(dash + 1));
        } catch (NoSuchElementException e) {
          throw new BadRequestException("No such downsampling function: "
                                        + parts[1].substring(dash + 1));
        }
        final int interval = HttpQuery.parseDuration(parts[1].substring(0, dash));
        tsdbquery.downsample(interval, downsampler);
      }
      tsdbqueries[nqueries++] = tsdbquery;
    }
    return tsdbqueries;
  }

  /**
   * Returns the aggregator with the given name.
   * @param name Name of the aggregator to get.
   * @throws BadRequestException if there's no aggregator with this name.
   */
  private static final Aggregator getAggregator(final String name) {
    try {
      return Aggregators.get(name);
    } catch (NoSuchElementException e) {
      throw new BadRequestException("No such aggregation function: " + name);
    }
  }
}

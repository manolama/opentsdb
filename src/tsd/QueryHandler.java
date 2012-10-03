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
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.formatters.Ascii;
import net.opentsdb.formatters.CollectdJSON;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.formatters.TsdbJSON;
import net.opentsdb.graph.GnuGraphFormatter;
import net.opentsdb.uid.NoSuchUniqueName;

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
    if (!nocache && query.getCacheAndReturn(query_hash)){
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

    if (tsdbqueries == null || total_queries < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the query");
      return;
    }
    
    // setup the proper formatter object based on the path
    String endpoint = query.getEndpoint();
    final TSDFormatter formatter;
    if (endpoint != null){
      if (endpoint.compareTo("ascii") == 0){
        formatter = new Ascii(tsdb);
      }else if (endpoint.compareTo("collectdjson") == 0){
        formatter = new CollectdJSON(tsdb);
      }else if (endpoint.compareTo("gnugraph") == 0){
        formatter = new GnuGraphFormatter(tsdb);
        // gnugraph needs more cruft set
        ((GnuGraphFormatter)formatter).init();
        ((GnuGraphFormatter)formatter).setBasePath(basepath);
        ((GnuGraphFormatter)formatter).setStartTime(start_time);
        ((GnuGraphFormatter)formatter).setEndTime(end_time);
        ((GnuGraphFormatter)formatter).setQueryString(query.querystring);
        ((GnuGraphFormatter)formatter).setQueryHash(query.hashCode());
      }else{
        formatter = new TsdbJSON(tsdb);
      }
    }else
      formatter = new TsdbJSON(tsdb);

    final int nqueries = tsdbqueries.length;
    LOG.trace(String.format("Number of queries [%d]", nqueries));
    for (int i = 0; i < nqueries; i++) {
      try {  // execute the TSDB query!
        // XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
        // TODO(tsuna): Optimization: run each query in parallel.
        final DataPoints[] series = tsdbqueries[i].run();
        
        // loop through the series and add them to the formatter
        for (final DataPoints datapoints : series) {
          formatter.putDatapoints(datapoints);
        }
      } catch (RuntimeException e) {
        LOG.info("Query failed (stack trace coming): " + tsdbqueries[i]);
        throw e;
      }
      tsdbqueries[i] = null;  // free()
    }
    tsdbqueries = null;  // free()
    
    // process the formatter
    formatter.handleHTTPGet(query);

    return;
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

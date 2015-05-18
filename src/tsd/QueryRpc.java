// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Query;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.DateTime;

/**
 * Handles queries for timeseries datapoints. Each request is parsed into a
 * TSQuery object, the values given validated, and if all tests pass, the
 * query is converted into TsdbQueries and each one is executed to fetch the
 * data. The resulting DataPoints[] are then passed to serializers for 
 * formatting.
 * <p>
 * Some private methods are included for parsing query string data into a 
 * TSQuery object.
 * @since 2.0
 */
final class QueryRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(QueryRpc.class);
  
  /**
   * Implements the /api/query endpoint to fetch data from OpenTSDB.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query for parsing and responding
   */
  @Override
  public void execute(final TSDB tsdb, final HttpQuery query) 
    throws IOException {
    
    // only accept GET/POST
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : ""; 
    
    if (endpoint.toLowerCase().equals("last")) {
      handleLastDataPointQuery(tsdb, query);
    } else {
      handleQuery(tsdb, query);
    }
  }

  /**
   * Processing for a data point query
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   */
  private void handleQuery(final TSDB tsdb, final HttpQuery query) {
    final long start = DateTime.currentTimeMillis();
    final TSQuery data_query;
    if (query.method() == HttpMethod.POST) {
      switch (query.apiVersion()) {
      case 0:
      case 1:
        data_query = query.serializer().parseQueryV1();
        break;
      default: 
        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
            "Requested API version not implemented", "Version " + 
            query.apiVersion() + " is not implemented");
      }
    } else {
      data_query = this.parseQuery(tsdb, query);
    }
    
    // validate and then compile the queries
    try {
      LOG.debug(data_query.toString());
      data_query.validateAndSetQuery();
    } catch (Exception e) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          e.getMessage(), data_query.toString(), e);
    }
    
    // if the user tried this query multiple times from the same IP and src port
    // they'll be rejected on subsequent calls
    final QueryStats query_stats = 
        new QueryStats(query.getRemoteAddress(), data_query);
    data_query.setQueryStats(query_stats);
    
    final int nqueries = data_query.getQueries().size();
    final ArrayList<DataPoints[]> results = new ArrayList<DataPoints[]>(nqueries);
    final List<Annotation> globals = new ArrayList<Annotation>();
    
    /** This has to be attached to callbacks or we may never respond to clients */
    class ErrorCB implements Callback<Object, Exception> {
      public Object call(final Exception e) throws Exception {
        try {
          if (e instanceof DeferredGroupException) {
            Throwable ex = e.getCause();
            while (ex != null && ex instanceof DeferredGroupException) {
              ex = ex.getCause();
            }
            if (ex != null) {
              if (ex instanceof NoSuchUniqueName) {
                query_stats.markComplete(HttpResponseStatus.BAD_REQUEST, ex);
                query.badRequest(new BadRequestException(
                    HttpResponseStatus.NOT_FOUND, ex.getMessage()));
                return null;
              }
              LOG.error("Query failed", ex);
              query_stats.markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
              query.badRequest(new BadRequestException(ex));
            } else {
              LOG.error("Unable to find the cause of the DGE", e);
              query_stats.markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
              query.badRequest(new BadRequestException(e));
            }
          } else if (e.getClass() == QueryException.class) {
            query_stats.markComplete(HttpResponseStatus.REQUEST_TIMEOUT, e);
            query.badRequest(new BadRequestException((QueryException)e));
          } else {
            LOG.error("Query failed", e);
            query_stats.markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
            query.badRequest(new BadRequestException(e));
          }
          return null;
        } catch (RuntimeException ex) {
          LOG.error("Exception thrown during exception handling", ex);
          query_stats.markComplete(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          query.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              ex.getMessage().getBytes());
          return null;
        }
      }
    }
    
    /**
     * After all of the queries have run, we get the results in the order given
     * and add dump the results in an array
     */
    class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {
      public Object call(final ArrayList<DataPoints[]> query_results) 
        throws Exception {
        results.addAll(query_results);

        /** Simply returns the buffer once serialization is complete and logs it */
        class SendIt implements Callback<Object, ChannelBuffer> {
          public Object call(final ChannelBuffer buffer) throws Exception {
            query.sendReply(buffer);
            return null;
          }
        }

        query_stats.setTimeStorage(System.currentTimeMillis() - start);
        switch (query.apiVersion()) {
        case 0:
        case 1:
            query.serializer().formatQueryAsyncV1(data_query, results, 
               globals).addCallback(new SendIt()).addErrback(new ErrorCB());
          break;
        default: 
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
              "Requested API version not implemented", "Version " + 
              query.apiVersion() + " is not implemented");
        }
        return null;
      }
    }
    
    /**
     * Callback executed after we have resolved the metric, tag names and tag
     * values to their respective UIDs. This callback then runs the actual 
     * queries and fetches their results.
     */
    class BuildCB implements Callback<Deferred<Object>, Query[]> {
      @Override
      public Deferred<Object> call(final Query[] queries) {
        final ArrayList<Deferred<DataPoints[]>> deferreds = 
            new ArrayList<Deferred<DataPoints[]>>(queries.length);
        for (final Query query : queries) {
          deferreds.add(query.runAsync());
        }
        return Deferred.groupInOrder(deferreds).addCallback(new QueriesCB());
      }
    }
    
    /** Handles storing the global annotations after fetching them */
    class GlobalCB implements Callback<Object, List<Annotation>> {
      public Object call(final List<Annotation> annotations) throws Exception {
        globals.addAll(annotations);
        return data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB());
      }
    }
 
    // if we the caller wants to search for global annotations, fire that off 
    // first then scan for the notes, then pass everything off to the formatter
    // when complete
    if (!data_query.getNoAnnotations() && data_query.getGlobalAnnotations()) {
      Annotation.getGlobalAnnotations(tsdb, 
        data_query.startTime() / 1000, data_query.endTime() / 1000)
          .addCallback(new GlobalCB()).addErrback(new ErrorCB());
    } else {
      data_query.buildQueriesAsync(tsdb).addCallback(new BuildCB())
        .addErrback(new ErrorCB());
    }
  }
  
  /**
   * Returns the last data point for each sub query if found.
   * @param tsdb The TSDB to which we belong
   * @param query The HTTP query to parse/respond
   */
  private void handleLastDataPointQuery(final TSDB tsdb, final HttpQuery query) {
    
    final net.opentsdb.core.LastPointQuery data_query;
    if (query.method() == HttpMethod.POST) {
      switch (query.apiVersion()) {
      case 0:
      case 1:
        data_query = query.serializer().parseLastPointQueryV1();
        break;
      default: 
        throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
            "Requested API version not implemented", "Version " + 
            query.apiVersion() + " is not implemented");
      }
    } else {
      data_query = this.parseLastPointQuery(tsdb, query);
    }
    
    if (data_query.getQueries() == null || data_query.getQueries().isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
          "Missing sub queries");
    }
    
    class QueryCB implements Callback<Object, ArrayList<IncomingDataPoint>> {
      @Override
      public Object call(final ArrayList<IncomingDataPoint> dps) throws Exception {
        query.sendReply(query.serializer().formatLastPointQueryV1(dps));
        return null;
      }
    }
    
    data_query.runQueries(tsdb).addCallback(new QueryCB());
  }
  
  /**
   * Parses a query string legacy style query from the URI
   * @param tsdb The TSDB we belong to
   * @param query The HTTP Query for parsing
   * @return A TSQuery if parsing was successful
   * @throws BadRequestException if parsing was unsuccessful
   */
  private TSQuery parseQuery(final TSDB tsdb, final HttpQuery query) {
    final TSQuery data_query = new TSQuery();
    
    data_query.setStart(query.getRequiredQueryStringParam("start"));
    data_query.setEnd(query.getQueryStringParam("end"));
    
    if (query.hasQueryStringParam("padding")) {
      data_query.setPadding(true);
    }
    
    if (query.hasQueryStringParam("no_annotations")) {
      data_query.setNoAnnotations(true);
    }
    
    if (query.hasQueryStringParam("global_annotations")) {
      data_query.setGlobalAnnotations(true);
    }
    
    if (query.hasQueryStringParam("show_tsuids")) {
      data_query.setShowTSUIDs(true);
    }
    
    if (query.hasQueryStringParam("ms")) {
      data_query.setMsResolution(true);
    }
    
    if (query.hasQueryStringParam("show_query")) {
      data_query.setShowQuery(true);
    }  
    
    if (query.hasQueryStringParam("show_stats")) {
      data_query.setShowStats(true);
    }    
    
    if (query.hasQueryStringParam("show_summary")) {
        data_query.setShowSummary(true);
    }
    
    // handle tsuid queries first
    if (query.hasQueryStringParam("tsuid")) {
      final List<String> tsuids = query.getQueryStringParams("tsuid");     
      for (String q : tsuids) {
        this.parseTsuidTypeSubQuery(q, data_query);
      }
    }
    
    if (query.hasQueryStringParam("m")) {
      final List<String> legacy_queries = query.getQueryStringParams("m");      
      for (String q : legacy_queries) {
        this.parseMTypeSubQuery(q, data_query);
      }
    }
    
    if (data_query.getQueries() == null || data_query.getQueries().size() < 1) {
      throw new BadRequestException("Missing sub queries");
    }
    return data_query;
  }
  
  /**
   * Parses a query string "m=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws BadRequestException if we are unable to parse the query or it is
   * missing components
   */
  private void parseMTypeSubQuery(final String query_string, 
      TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new BadRequestException("The query string was empty");
    }
    
    // m is of the following forms:
    // agg:[interval-agg:][rate:]metric[{tag=value,...}]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = Tags.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new BadRequestException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
    }
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    HashMap<String, String> tags = new HashMap<String, String>();
    sub_query.setMetric(Tags.parseWithMetric(parts[i], tags));
    sub_query.setTags(tags);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses a "tsuid=..." type query and adds it to the TSQuery.
   * This will generate a TSSubQuery and add it to the TSQuery if successful
   * @param query_string The value of the m query string parameter, i.e. what
   * comes after the equals sign
   * @param data_query The query we're building
   * @throws BadRequestException if we are unable to parse the query or it is
   * missing components
   */
  private void parseTsuidTypeSubQuery(final String query_string, 
      TSQuery data_query) {
    if (query_string == null || query_string.isEmpty()) {
      throw new BadRequestException("The tsuid query string was empty");
    }
    
    // tsuid queries are of the following forms:
    // agg:[interval-agg:][rate:]tsuid[,s]
    // where the parts in square brackets `[' .. `]' are optional.
    final String[] parts = Tags.splitString(query_string, ':');
    int i = parts.length;
    if (i < 2 || i > 5) {
      throw new BadRequestException("Invalid parameter m=" + query_string + " ("
          + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
    }
    
    final TSSubQuery sub_query = new TSSubQuery();
    
    // the aggregator is first
    sub_query.setAggregator(parts[0]);
    
    i--; // Move to the last part (the metric name).
    final List<String> tsuid_array = Arrays.asList(parts[i].split(","));
    sub_query.setTsuids(tsuid_array);
    
    // parse out the rate and downsampler 
    for (int x = 1; x < parts.length - 1; x++) {
      if (parts[x].toLowerCase().startsWith("rate")) {
        sub_query.setRate(true);
        if (parts[x].indexOf("{") >= 0) {
          sub_query.setRateOptions(QueryRpc.parseRateOptions(true, parts[x]));
        }
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[x]);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
  
  /**
   * Parses the "rate" section of the query string and returns an instance
   * of the RateOptions class that contains the values found.
   * <p/>
   * The format of the rate specification is rate[{counter[,#[,#]]}].
   * @param rate If true, then the query is set as a rate query and the rate
   * specification will be parsed. If false, a default RateOptions instance
   * will be returned and largely ignored by the rest of the processing
   * @param spec The part of the query string that pertains to the rate
   * @return An initialized RateOptions instance based on the specification
   * @throws BadRequestException if the parameter is malformed
   * @since 2.0
   */
   static final public RateOptions parseRateOptions(final boolean rate,
       final String spec) {
     if (!rate || spec.length() == 4) {
       return new RateOptions(false, Long.MAX_VALUE,
           RateOptions.DEFAULT_RESET_VALUE);
     }

     if (spec.length() < 6) {
       throw new BadRequestException("Invalid rate options specification: "
           + spec);
     }

     String[] parts = Tags
         .splitString(spec.substring(5, spec.length() - 1), ',');
     if (parts.length < 1 || parts.length > 3) {
       throw new BadRequestException(
           "Incorrect number of values in rate options specification, must be " +
           "counter[,counter max value,reset value], recieved: "
               + parts.length + " parts");
     }

     final boolean counter = parts[0].endsWith("counter");
     try {
       final long max = (parts.length >= 2 && parts[1].length() > 0 ? Long
           .parseLong(parts[1]) : Long.MAX_VALUE);
       try {
         final long reset = (parts.length >= 3 && parts[2].length() > 0 ? Long
             .parseLong(parts[2]) : RateOptions.DEFAULT_RESET_VALUE);
         final boolean drop_counter = parts[0].equals("dropcounter");
         return new RateOptions(counter, max, reset, drop_counter);
       } catch (NumberFormatException e) {
         throw new BadRequestException(
             "Reset value of counter was not a number, received '" + parts[2]
                 + "'");
       }
     } catch (NumberFormatException e) {
       throw new BadRequestException(
           "Max value of counter was not a number, received '" + parts[1] + "'");
     }
   }

  /**
   * Parses a last point query from the URI string
   * @param tsdb The TSDB to which we belong
   * @param http_query The HTTP query to work with
   * @return A LastPointQuery object to execute against
   * @throws BadRequestException if parsing failed
   */
  private net.opentsdb.core.LastPointQuery parseLastPointQuery(final TSDB tsdb, 
      final HttpQuery http_query) {
    final net.opentsdb.core.LastPointQuery query = 
        new net.opentsdb.core.LastPointQuery();
    
    if (http_query.hasQueryStringParam("resolve")) {
      query.setResolveNames(true);
    }
    
    if (http_query.hasQueryStringParam("back_scan")) {
      try {
        query.setBackScan(Integer.parseInt(http_query.getQueryStringParam("back_scan")));
      } catch (NumberFormatException e) {
        throw new BadRequestException("Unable to parse back_scan parameter");
      }
    }
    
    final List<String> ts_queries = http_query.getQueryStringParams("timeseries");
    final List<String> tsuid_queries = http_query.getQueryStringParams("tsuids");
    final int num_queries = 
      (ts_queries != null ? ts_queries.size() : 0) +
      (tsuid_queries != null ? tsuid_queries.size() : 0);
    final List<net.opentsdb.core.LastPointSubQuery> sub_queries = 
      new ArrayList<net.opentsdb.core.LastPointSubQuery>(num_queries);
    
    if (ts_queries != null) {
      for (String ts_query : ts_queries) {
        sub_queries.add(
            net.opentsdb.core.LastPointSubQuery.parseTimeSeriesQuery(ts_query));
      }
    }
    
    if (tsuid_queries != null) {
      for (String tsuid_query : tsuid_queries) {
        sub_queries.add(
            net.opentsdb.core.LastPointSubQuery.parseTSUIDQuery(tsuid_query));
      }
    }
    
    query.setQueries(sub_queries);
    return query;
  }
  
  /**
   * @deprecated Use {@see net.opentsdb.core.LastPointQuery} instead!!!
   * This class will be removed in 2.3
   */
  public static class LastPointQuery extends net.opentsdb.core.LastPointQuery {
    // DIE DIE DIE! 
  }
  
  /**
   * @deprecated Use {@see net.opentsdb.core.LastPointSubQuery} instead!!!
   * This class will be removed in 2.3
   */
  public static class LastPointSubQuery extends 
    net.opentsdb.core.LastPointSubQuery {
    // DIE DIE DIE! 
  }
}
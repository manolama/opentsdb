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

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.Annotation;

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
    data_query.validateAndSetQuery();    
    Query[] tsdbqueries = data_query.buildQueries(tsdb);
    final int nqueries = tsdbqueries.length;
    final ArrayList<DataPoints[]> results = 
      new ArrayList<DataPoints[]>(nqueries);
    final ArrayList<Deferred<DataPoints[]>> deferreds =
      new ArrayList<Deferred<DataPoints[]>>(nqueries);
    
    for (int i = 0; i < nqueries; i++) {
      deferreds.add(tsdbqueries[i].runAsync());
    }
    
    // if the user wants global annotations, we need to scan and fetch
    final List<Annotation> globals = new ArrayList<Annotation>(0);

    /**
     * This callback is used to keep only the results from queries that actually
     * return some data. It should be added to the grouped response to the
     * deferreds array list.
     */
    class QueriesCB implements Callback<Object, ArrayList<Object>> {

      @Override
      public Object call(final ArrayList<Object> query_results) throws Exception {
        for (Object obj : query_results) {
          final DataPoints[] dps = (DataPoints[])obj;
          if (dps.length > 0) {
            results.add(dps);
          }
        }
        return null;
      }
      
    }
    
    /**
     * Used to copy the results of a global annotation lookup to the local
     * {@code globals} list. The list must be final to be accessible from the
     * serialization code below.
     */
    class AnnotationsCB implements Callback<Object, List<Annotation>> {

      @Override
      public Object call(final List<Annotation> annotations) throws Exception {
        globals.addAll(annotations);
        return null;
      }
      
    }

    // we have to join on the deferred groups for now until the metric and tag
    // calls are asynced in DataPoints. Otherwise if we try to place the
    // serialization code in a callback, it will hang the query
    if (!data_query.getNoAnnotations() && data_query.getGlobalAnnotations()) {
      final Deferred<List<Annotation>> gbs = Annotation.getGlobalAnnotations(tsdb, 
          data_query.startTime() / 1000, data_query.endTime() / 1000);
      final ArrayList<Deferred<Object>> dfs =
        new ArrayList<Deferred<Object>>(2);
      dfs.add(Deferred.group(deferreds).addCallback(new QueriesCB()));
      dfs.add(gbs.addCallback(new AnnotationsCB()));
      try {
        Deferred.group(dfs).joinUninterruptibly();
      } catch (Exception e) {
        throw new RuntimeException("Shouldn't be here", e);
      }
    } else {
      try {
        Deferred.group(deferreds).addCallback(new QueriesCB()).joinUninterruptibly();
      } catch (Exception e) {
        throw new RuntimeException("Shouldn't be here", e);
      }
    }
    
    switch (query.apiVersion()) {
    case 0:
    case 1:
      query.sendReply(query.serializer().formatQueryV1(data_query, results, 
          null));
      break;
    default: 
      throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Requested API version not implemented", "Version " + 
          query.apiVersion() + " is not implemented");
    }
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
    
    if (query.hasQueryStringParam("ms")) {
      data_query.setMsResolution(true);
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
      if (parts[x].toLowerCase().equals("rate")) {
        sub_query.setRate(true);
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[1]);
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
      if (parts[x].toLowerCase().equals("rate")) {
        sub_query.setRate(true);
      } else if (Character.isDigit(parts[x].charAt(0))) {
        sub_query.setDownsample(parts[1]);
      }
    }
    
    if (data_query.getQueries() == null) {
      final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
      data_query.setQueries(subs);
    }
    data_query.getQueries().add(sub_query);
  }
}

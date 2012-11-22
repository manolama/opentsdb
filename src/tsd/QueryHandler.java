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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.JSON;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;

/**
 * Used to be the GraphHandler, but we won't always be requesting graphs. Instead we'll
 * do the heavy lifting of parsing the query (the {@code /q} endpoint) here and
 * if the user requests it, we'll pass it on to the graph handler. Otherwise we'll just
 * return some JSON for the querent to play with
 */
public class QueryHandler implements HttpRpc {
  
  /** Used for deserializing the Collectd JSON data */
  private static final TypeReference<DataQuery> dqTypeRef = 
    new TypeReference<DataQuery>() {
  };
  
  private static final Logger LOG = LoggerFactory.getLogger(QueryHandler.class);

  /** 
   * Checks the cache first for valid data, then performs one or more queries against
   * HBase to fetch data, and stores data in the cache if applicable.
   * @param tsdb The TSDB to use for fetching data
   * @param query The HTTP query to work with
   * @throws IOException 
   */
  public void execute(final TSDB tsdb, final HttpQuery query) throws IOException {
    if (TSDB.role == TSDRole.Ingest){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + TSDB.role + "]");
      return;
    }
    
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    
//    if (!nocache){
//      try{
//        @SuppressWarnings("unchecked")
//        List<DataPoints> cached = (List<DataPoints>)tsdb.cache.get(CacheRegion.QUERY, query_hash);
//        if (cached != null){
//          LOG.trace("was cached");
//          query.getFormatter().putDatapoints(cached);
//          query.getFormatter().handleHTTPDataGet(query);
//          return;
//        }
//      }catch (Exception e){
//        e.printStackTrace();
//      }
//    }
    
    // parse query
    DataQuery dq = null;
    if (query.getMethod() == HttpMethod.POST){
      LOG.trace("Parsing POST data: " + query.getPostData());
      JSON codec = new JSON(new DataQuery());
      if (!codec.parseObject(query.getPostData(), dqTypeRef)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data: " + codec.getError());
        return;
      }
      dq = (DataQuery)codec.getObject();
      LOG.trace(codec.getJsonString());
      if (!dq.parseQuery(tsdb, query)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
        return;
      }
    }else{
      dq = new DataQuery();
      if (!dq.parseQueryString(tsdb, query)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
        return;
      }
    }    
   
    dq.query_hash = query.hashCode();
    
    // determine how many HBase queries we'll need to run
    Query[] tsdbqueries = dq.getTSDQueries();
    
    // validate the query before running it
    if (!query.getFormatter().validateQuery(dq)){
      query.sendError(HttpResponseStatus.BAD_REQUEST, dq.error);
      return;
    }

    if (tsdbqueries == null || tsdbqueries.length < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the query");
      return;
    }
    
    final int nqueries = tsdbqueries.length;
    LOG.trace(String.format("Number of queries [%d]", nqueries));
    for (int i = 0; i < nqueries; i++) {
      try {  // execute the TSDB query!
        // XXX This is slow and will block Netty.  TODO(tsuna): Don't block.
        // TODO(tsuna): Optimization: run each query in parallel.
        final DataPoints[] series = tsdbqueries[i].run();
        
        // loop through the series and add them to the formatter
        for (final DataPoints datapoints : series) {
          query.getFormatter().putDatapoints(datapoints);
        }
      } catch (RuntimeException e) {
        LOG.info("Query failed (stack trace coming): " + tsdbqueries[i]);
        throw e;
      }
      tsdbqueries[i] = null;  // free()
    }
    tsdbqueries = null;  // free()
    
    // if this was a topn query, we need to filter it before calling the formatter's function
    if (dq.topn != null){
      LOG.trace("Filtering on topn");
      
      TreeMap<Double, Integer> temp = (dq.topn.reverse ? 
          new TreeMap<Double, Integer>(Collections.reverseOrder()) : 
            new TreeMap<Double, Integer>());
      int i=0;
      for (DataPoints dps : query.getFormatter().getDataPoints()){
        temp.put(dps.doubleValue(0), i);
        i++;
      }
      
      //SortedMap<Double, Integer> sorted = (SortedMap<Double, Integer>)temp;
      List<DataPoints> series = new ArrayList<DataPoints>(temp.size());
      i = 0;
      for (Map.Entry<Double, Integer> entry : temp.entrySet()){
        if (i >= dq.topn.limit)
          break;
        series.add(query.getFormatter().getDataPoints().get(entry.getValue()));
        i++;
      }
      
      query.getFormatter().putDatapoints(series);
    }
    
//    if (!nocache)
//      tsdb.cache.put(CacheRegion.QUERY, query_hash, query.getFormatter().getDataPoints());
    
    // process the formatter
    query.getFormatter().handleHTTPDataGet(query);
    return;
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.GeneralMeta.Meta_Type;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;

import org.apache.lucene.document.Document;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles methods for editing, fetching and searching the metadata objects
 * stored in the TSDB UID table.
 * 
 */
public class MetaRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(MetaRPC.class);

  /**
   * Routes the query to the proper handler
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    if (tsdb.role != TSDRole.API){
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented for role [" + tsdb.role + "]");
      return;
    }
    
    LOG.trace(query.request.getUri());
    // GET
    if (query.getMethod() == HttpMethod.GET) {
      this.getMeta(tsdb, query);
      return;
      // POST
    } else if (query.getMethod() == HttpMethod.POST ||
        query.getMethod() == HttpMethod.PUT ) {    
      
      final String endpoint = query.getEndpoint();
      if (endpoint == null || endpoint.isEmpty()){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing proper endpoint");
        return;
      }

      if (endpoint.compareTo("timeseries") == 0)
        this.postTimeseries(tsdb, query);
      else if (endpoint.compareTo("metric") == 0)
        this.postGeneral(tsdb, query, Meta_Type.METRICS);
      else if (endpoint.compareTo("tagk") == 0)
        this.postGeneral(tsdb, query, Meta_Type.TAGK);
      else if (endpoint.compareTo("tagv") == 0)
        this.postGeneral(tsdb, query, Meta_Type.TAGV);
      
    } else {
      query.sendError(HttpResponseStatus.METHOD_NOT_ALLOWED,
          String.format("Method [%s] is not allowed", query.getMethod()));
      return;
    }
  }

  /**
   * Handles GET requests for the meta RPC
   * @param tsdb The TSDB to use for fetching/writing meta data
   * @param query The Query to respond to
   */
  private void getMeta(final TSDB tsdb, final HttpQuery query) { 
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    final String endpoint = query.getEndpoint();
    if (endpoint == null || endpoint.isEmpty() || endpoint.toLowerCase().compareTo("meta") == 0){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing type of meta endpoint");
      return;
    }      
    if (!query.hasQueryStringParam("uid")) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing [uid] parameter");
      return;
    }
    byte[] id = UniqueId.StringtoID(query.getQueryStringParam("uid"));

    Object meta = null;
    if (endpoint == null || endpoint.isEmpty() || endpoint.compareTo("timeseries") == 0){
      meta = tsdb.getTimeSeriesMeta(id, true);
    } else if (endpoint.compareTo("metric") == 0){
      meta = tsdb.metrics.getGeneralMeta(id, false);
    } else if (endpoint.compareTo("tagk") == 0){
      meta = tsdb.tag_names.getGeneralMeta(id, false);
    } else if (endpoint.compareTo("tagv") == 0){
      meta = tsdb.tag_values.getGeneralMeta(id, false);
    }

    if (meta == null){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "UID object was not found");
      return;
    }
    formatter.handleHTTPMetaGet(query, meta);
  }

  /**
   * Handles POST requests for the meta RPC
   * @param tsdb The TSDB to use for fetching/writing meta data
   * @param query The Query to respond to
   */
  private void postGeneral(final TSDB tsdb, final HttpQuery query, 
      final Meta_Type type) {
    final String content = query.getPostData();    
    GeneralMeta meta = new GeneralMeta();

    // if it's not a true post, parse the query string for the data
    if (content.length() < 1) {
      // check query string for values
      if (!query.hasQueryStringParam("uid")) {
        query.sendError(HttpResponseStatus.BAD_REQUEST,
            "Missing [uid] parameter");
        return;
      }

      meta.setUID(query.getQueryStringParam("uid"));
      if (query.hasQueryStringParam("display_name"))
        meta.setDisplay_name(query.getQueryStringParam("display_name"));
      if (query.hasQueryStringParam("description"))
        meta.setDescription(query.getQueryStringParam("description"));
      if (query.hasQueryStringParam("notes"))
        meta.setNotes(query.getQueryStringParam("notes"));
      
      // custom tags are &key1=name&val1=value
      int i = 1;
      Map<String, String> custom = new HashMap<String, String>();
      while (query.hasQueryStringParam("key" + i)) {
        if (!query.hasQueryStringParam("val" + i)) {
          LOG.trace(String.format(
                  "Breaking because [key%d] does not have a corresponding value",
                  i));
          break;
        }
        final String key = query.getQueryStringParam("key" + i);
        final String val = query.getQueryStringParam("val" + i);
        custom.put(key, val);
        i++;
      }
      if (custom.size() > 0)
        meta.setCustom(custom);
    } else {
      // parse
      JSON parser = new JSON(meta);
      if (!parser.parseObject(content)) {
        LOG.error("Couldn't parse JSON content");
        LOG.trace(content);
        query.sendError(HttpResponseStatus.UNPROCESSABLE_ENTITY,
            parser.getError());
        return;
      }
      meta = (GeneralMeta) parser.getObject();
    }

    // sanity checks
    if (meta.getUID().isEmpty()) {
      query
          .sendError(HttpResponseStatus.BAD_REQUEST, "Missing [uid] parameter");
      return;
    }

    try{
      // put it in the right place
      switch (type) {
      case METRICS:
        meta.setName(tsdb.metrics.getName(UniqueId.StringtoID(meta.getUID())));
        meta = tsdb.metrics.putMeta(meta, true);
        if (meta == null) {
          query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Error saving data");
          return;
        }
        break;
      case TAGK:
        meta.setName(tsdb.tag_names.getName(UniqueId.StringtoID(meta.getUID())));
        meta = tsdb.tag_names.putMeta(meta, true);
        if (meta == null) {
          query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Error saving data");
          return;
        }
        break;
      case TAGV:
        meta.setName(tsdb.tag_values.getName(UniqueId.StringtoID(meta.getUID())));
        meta = tsdb.tag_values.putMeta(meta, true);
        if (meta == null) {
          query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Error saving data");
          return;
        }

        break;
      }
    } catch (NoSuchUniqueId nsui){
      LOG.warn(String.format("No UID found for [%s]", meta.getUID()));
      query.sendError(HttpResponseStatus.BAD_REQUEST, "UID was not found");
    }

    JSON parser = new JSON(meta);
    query.sendReply(parser.getJsonString());
    return;
  }
  
  /**
   * Handles POST requests for the meta RPC
   * @param tsdb The TSDB to use for fetching/writing meta data
   * @param query The Query to respond to
   */
  private void postTimeseries(final TSDB tsdb, final HttpQuery query) {
    final String content = query.getPostData();    
    TimeSeriesMeta ts_meta = new TimeSeriesMeta();

    // if it's not a true post, parse the query string for the data
    if (content.length() < 1) {
      // check query string for values
      if (!query.hasQueryStringParam("uid")) {
        query.sendError(HttpResponseStatus.BAD_REQUEST,
            "Missing [uid] parameter");
        return;
      }

      if (query.hasQueryStringParam("notes"))
        ts_meta.setNotes(query.getQueryStringParam("notes"));
      if (query.hasQueryStringParam("retention")){
        final int retention = Integer.parseInt(query
            .getQueryStringParam("retention"));
        ts_meta.setRetention(retention);
      }
      if (query.hasQueryStringParam("max")){
        final double max = Double.parseDouble(query
            .getQueryStringParam("max"));
        ts_meta.setMax(max);
      }
      if (query.hasQueryStringParam("min")){
        final double min = Double.parseDouble(query
            .getQueryStringParam("min"));
        ts_meta.setMax(min);
      }
      if (query.hasQueryStringParam("units"))
        ts_meta.setUnits(query.getQueryStringParam("units"));
      if (query.hasQueryStringParam("data_type")){
        final int retention = Integer.parseInt(query
            .getQueryStringParam("data_type"));
        ts_meta.setRetention(retention);
      }
      
      // custom tags are &key1=name&val1=value
      int i = 1;
      Map<String, String> custom = new HashMap<String, String>();
      while (query.hasQueryStringParam("key" + i)) {
        if (!query.hasQueryStringParam("val" + i)) {
          LOG.trace(String.format(
                  "Breaking because [key%d] does not have a corresponding value",
                  i));
          break;
        }
        final String key = query.getQueryStringParam("key" + i);
        final String val = query.getQueryStringParam("val" + i);
        custom.put(key, val);
        i++;
      }
      if (custom.size() > 0)
        ts_meta.setCustom(custom);
    } else {
      // parse
      JSON parser = new JSON(ts_meta);
      if (!parser.parseObject(content)) {
        LOG.error("Couldn't parse JSON content");
        LOG.trace(content);
        query.sendError(HttpResponseStatus.UNPROCESSABLE_ENTITY,
            parser.getError());
        return;
      }
      ts_meta = (TimeSeriesMeta)parser.getObject();
    }

    // sanity checks
    if (ts_meta.getUID().isEmpty()) {
      query
          .sendError(HttpResponseStatus.BAD_REQUEST, "Missing [uid] parameter");
      return;
    }

    if (!tsdb.putMeta(ts_meta)){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Error saving data");
      return;
    }
    ts_meta = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(ts_meta.getUID()), true);
    
    // update the search index
    Document doc = ts_meta.buildLuceneDoc();
    if (doc == null)
      LOG.warn(String.format("Unable to get Lucene doc for TSUID [%s]", ts_meta.getUID()));
    else
      tsdb.meta_search_writer.index(doc, "tsuid");
    
    JSON parser = new JSON(ts_meta);
    query.sendReply(parser.getJsonString());
    return;
  }
}
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

/**
 * Abstract base class for Formatters; plugins that handle converting requests
 * and responses between OpenTSDB's internal data and various popular formats
 * such as JSON, XML, OData, etc. They can also be used to accept inputs from 
 * existing collection systems such as CollectD.
 * <p>
 * The formatter workflow is as follows:
 * <ul><li>Request comes in via the HTTP API</li>
 * <li>The proper formatter is instantiated via:
 * <ul><li>Query string parameter "formatter=&lt;shortName&gt;"</li>
 * <li>If no query string parameter is found, the Content-Type is parsed</li>
 * <li>Otherwise the default formatter is used</li></ul></li>
 * <li>The request is routed to an RPC handler</li>
 * <li>If the handler needs details for a complex request, it calls on the 
 * proper formatter's "parseX" method to get a query object</li>
 * <li>The RPC handler fetches and organizes the data</li>
 * <li>The handler passes the data to the proper formatter's "formatX" 
 * method</li>
 * <li>The formatter formats the data and sends it back as a byte array</li>
 * </ul>
 * <p>
 * Implementations only need to override the methods they want to change. For 
 * example, CollectD can send metrics in a JSON format but it's not what 
 * OpenTSDB's "json" formatter expects. The CollectD plugin can implement only
 * the "parsePut()" method to convert the data for storage.
 * <p>
 * <b>Warning:</b> Every HTTP request will instantiate a new formatter object
 * (except for a few that don't require it) so please avoid creating heavy
 * objects in the constructor, parse or format methods. Instead, use the 
 * {@link initialize} method to instantiate thread-safe, static objects that
 * you need for de/serializtion. It will be called once on TSD startup.
 * <p>
 * <b>Note:</b> If a method needs to throw an exception due to user error, such
 * as missing data or a bad request, throw a {@link BadRequestException} with
 * a status code, error message and optional details.
 * <p>
 * <b>Note:</b> You can change the HTTP status code before returning from a 
 * "formatX" method by accessing "this.query.response().setStatus()" and
 * providing an {@link HttpResponseStatus} object.
 * <p>
 * <b>Note:</b> You can also set response headers via 
 * "this.query.response().setHeader()". The "Content-Type" header will be set
 * automatically with the "response_content_type" field value that can be
 * overridden by the plugin. HttpQuery will also set some other headers before
 * returning
 * @since 2.0
 */
public abstract class HttpFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(HttpFormatter.class);
  
  /** Content type to use for matching a formatter to incoming requests */
  protected String request_content_type = "application/json";
  
  /** Content type to return with data from this formatter */
  protected String response_content_type = "application/json; charset=UTF-8";
  
  /** The query used for accessing the DefaultHttpResponse object and other 
   * information */
  protected final HttpQuery query;
  
  /**
   * Empty constructor required for plugin operation
   */
  public HttpFormatter() {
    this.query = null;
  }

  /**
   * Constructor that formatters must implement. This is how each plugin will 
   * get the request content and have the option to set headers or a custom
   * status code in the response.
   * <p>
   * <b>Note:</b> A new formatter is instantiated for every HTTP connection, so
   * don't do any heavy object creation here. Instead, use the {@link initialize} 
   * method to setup static, thread-safe objects if you need stuff like that 
   * @param query
   */
  public HttpFormatter(final HttpQuery query) {
    this.query = query;
  }
  
  /**
   * Initializer called one time when the TSD starts up and loads formatter 
   * plugins. You should use this method to setup static, thread-safe objects
   * required for parsing or formatting data. 
   * @param tsdb The TSD this plugin belongs to. Use it to fetch config data
   * if require.
   */
  public abstract void initialize(final TSDB tsdb);
  
  /**
   * Called when the TSD is shutting down so implementations can gracefully 
   * close their objects or connections if necessary
   * @return An object, usually a Boolean, used to wait on during shutdown
   */
  public abstract Deferred<Object> Shutdown();
  
  /** @return the version of this formatter plugin */
  public abstract String version();
  
  /**
   * The simple name for this formatter referenced by users.
   * The name should be lower case, all one word without any odd characters
   * so it can be used in a query string. E.g. "json" or "xml" or "odata"
   * @return the name of the formatter
   */
  public abstract String shortName();

  /** @return the incoming content type */
  public String requestContentType() {
    return this.request_content_type;
  }
  
  /** @return the outoing content type */
  public String responseContentType() {
    return this.response_content_type;
  }
  
  public ArrayList<IncomingDataPoint> parsePutV1() throws IOException {
    if (!query.hasContent())
      throw new BadRequestException("Missing request content");
    
  }
  
  /**
   * Parses a suggestion query
   * @return a hash map of key/value pairs
   * @throws IOException if the parsing failed
   */
  @SuppressWarnings("unchecked")
  public HashMap<String, String> parseSuggestV1() throws IOException {
    if (!query.hasContent())
      return null;
    return JSON.parseToObject(query.getContent(), HashMap.class);
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A JSON formatted byte array
   * @throws IOException if the serialization failed
   */
  public byte[] formatSuggestV1(final List<String> suggestions) 
    throws IOException {
    if (query.hasQueryStringParam("jsonp")) {
      return JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
          suggestions);
    }
    return JSON.serializeToBytes(suggestions);
  }
  
  /**
   * Formats a 404 error when an endpoint or file wasn't found
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @return A standard JSON error
   */
  public byte[] formatNotFoundV1() {
    HashMap<String, HashMap<String, Object>> map = 
      new HashMap<String, HashMap<String, Object>>();
    HashMap<String, Object> error = new HashMap<String, Object>();
    error.put("code", 404);
    if (query.apiVersion() > 0) {
      error.put("message", "Endpoint not found");
    } else {
      error.put("message", "Page not found");
    }
    error.put("details", "");
    map.put("error", error);
    try{
      if (query.hasQueryStringParam("jsonp")) {
        return JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
            map);
      }
      return JSON.serializeToBytes(map);
    } catch (IOException ioe) {
      LOG.error("Unable to serialize 404 Error");
      StringBuilder output = 
        new StringBuilder(1024);
      if (query.hasQueryStringParam("jsonp")) {
        output.append(query.getQueryStringParam("jsonp") + "(");
      }
      output.append("{\"error\":{\"code\":");
      output.append(404);
      output.append(",\"message\":\"");
      if (query.apiVersion() > 0) {
        output.append("Endpoint not found");
      } else {
        output.append("Page not found");
      }
      output.append("\",\"details\":\"").append("");
      output.append("\"}}");
      if (query.hasQueryStringParam("jsonp")) {
        output.append(")");
      }
      return output.toString().getBytes(this.query.getCharset());
    }
  }
  
  /**
   * Format a bad request exception, indicating an invalid request from the
   * user
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @param exception The exception to format
   * @return A standard JSON error
   */
  public byte[] formatErrorV1(final BadRequestException exception) {
    HashMap<String, HashMap<String, Object>> map = 
      new HashMap<String, HashMap<String, Object>>();
    HashMap<String, Object> error = new HashMap<String, Object>();
    error.put("code", exception.getStatus().getCode());
    error.put("message", exception.getMessage());
    error.put("details", exception.getDetails());
    if (query.showStackTrace()) {
      StringBuilder sb = new StringBuilder();
      for (StackTraceElement element : exception.getStackTrace()) {
          sb.append(element.toString());
          sb.append("\n");
      }
      error.put("trace", sb);
    }
    map.put("error", error);
    try{
      if (query.hasQueryStringParam("jsonp")) {
        return JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
            map);
      }
      return JSON.serializeToBytes(map);
    } catch (IOException ioe) {
      LOG.error("Unable to serialize BadRequest Exception", exception);
      StringBuilder output = 
        new StringBuilder(exception.getMessage().length() * 2);
      if (query.hasQueryStringParam("jsonp")) {
        output.append(query.getQueryStringParam("jsonp") + "(");
      }
      output.append("{\"error\":{\"code\":");
      output.append(exception.getStatus().getCode());
      output.append(",\"message\":\"").append(exception.getMessage());
      output.append("\",\"details\":\"").append(exception.getDetails());
      output.append("\"}}");
      if (query.hasQueryStringParam("jsonp")) {
        output.append(")");
      }
      return output.toString().getBytes(this.query.getCharset());
    }
  }
  
  /**
   * Format an internal error exception that was caused by the system
   * Should return a 500 error
   * <p>
   * <b>WARNING:</b> If overriding, make sure this method catches all errors and
   * returns a byte array with a simple string error at the minimum
   * @param exception The system exception to format
   * @return A standard JSON error
   */
  public byte[] formatErrorV1(final Exception exception) {
    HashMap<String, HashMap<String, Object>> map = 
      new HashMap<String, HashMap<String, Object>>();
    HashMap<String, Object> error = new HashMap<String, Object>();
    error.put("code", 500);
    error.put("message", exception.getMessage());
    error.put("details", "");
    if (query.showStackTrace()) {
      StringBuilder sb = new StringBuilder();
      for (StackTraceElement element : exception.getStackTrace()) {
          sb.append(element.toString());
          sb.append("\n");
      }
      error.put("trace", sb);
    }
    map.put("error", error);
    try{
      if (query.hasQueryStringParam("jsonp")) {
        return JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
            map);
      }
      return JSON.serializeToBytes(map);
    } catch (IOException ioe) {
      LOG.error("Unable to serialize BadRequest Exception", exception);
      StringBuilder output = 
        new StringBuilder(exception.getMessage().length() * 2);
      if (query.hasQueryStringParam("jsonp")) {
        output.append(query.getQueryStringParam("jsonp") + "(");
      }
      output.append("{\"error\":{\"code\":");
      output.append(500);
      output.append(",\"message\":\"").append(exception.getMessage());
      output.append("\",\"details\":\"").append("");
      output.append("\"}}");
      if (query.hasQueryStringParam("jsonp")) {
        output.append(")");
      }
      return output.toString().getBytes(this.query.getCharset());
    }
  }

  /**
   * Format the formatters status map
   * @return A JSON structure
   * @throws IOException if the serialization failed
   */
  public byte[] formatFormattersV1() throws IOException {
    if (query.hasQueryStringParam("jsonp")) {
      return JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
          HttpQuery.getFormatterStatus());
    }
    return JSON.serializeToBytes(HttpQuery.getFormatterStatus());
  }
}

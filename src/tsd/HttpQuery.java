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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.graph.Plot;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.HttpFormatter;
import net.opentsdb.utils.PluginLoader;

/**
 * Binds together an HTTP request and the channel on which it was received.
 *
 * It makes it easier to provide a few utility methods to respond to the
 * requests.
 */
final class HttpQuery {

  private static final Logger LOG = LoggerFactory.getLogger(HttpQuery.class);

  private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  /** The maximum implemented API version, set when the user doesn't */
  private static final int MAX_API_VERSION = 1;
  
  /**
   * Keep track of the latency of HTTP requests.
   */
  private static final Histogram httplatency =
    new Histogram(16000, (short) 2, 100);

  /** Maps Content-Type to a formatter @since 2.0 */
  private static HashMap<String, Constructor<? extends HttpFormatter>> 
    formatter_map_content_type = null;
  
  /** Maps query string names to a formatter @since 2.0 */
  private static HashMap<String, Constructor<? extends HttpFormatter>> 
    formatter_map_query_string = null;
  
  /** Caches formatter implementation information for user access @since 2.0 */
  private static ArrayList<HashMap<String, Object>> formatter_status = null;
  
  /** When the query was started (useful for timing). */
  private final long start_time = System.nanoTime();

  /** The request in this HTTP query. */
  private final HttpRequest request;

  /** The channel on which the request was received. */
  private final Channel chan;

  /** Shortcut to the request method */
  private final HttpMethod method; 
  
  /** Parsed query string (lazily built on first access). */
  private Map<String, List<String>> querystring;

  /** API version parsed from the incoming request */
  private int api_version = 0;
  
  /** The formatter to use for parsing input and responding */
  private HttpFormatter formatter = null;
  
  /** Deferred result of this query, to allow asynchronous processing.  */
  private final Deferred<Object> deferred = new Deferred<Object>();

  /** The response object we'll fill with data @since 2.0 */
  private final DefaultHttpResponse response =
    new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.ACCEPTED);
  
  /** The {@code TSDB} instance we belong to */
  private final TSDB tsdb; 
  
  /** Whether or not to show stack traces in the output @since 2.0 */
  private final boolean show_stack_trace;
  
  /**
   * Constructor.
   * @param request The request in this HTTP query.
   * @param chan The channel on which the request was received.
   */
  public HttpQuery(final TSDB tsdb, final HttpRequest request, final Channel chan) {
    this.tsdb = tsdb;
    this.request = request;
    this.chan = chan;
    this.show_stack_trace = 
      tsdb.getConfig().getBoolean("tsd.http.show_stack_trace");
    this.method = request.getMethod();
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.latency", httplatency, "type=all");
  }

  /**
   * Returns the underlying Netty {@link HttpRequest} of this query.
   */
  public HttpRequest request() {
    return request;
  }

  /** Returns the HTTP method/verb for the request */
  public HttpMethod method() {
    return this.method;
  }
  
  /** Returns the response object, allowing formatters to set headers @since 2.0 */
  public DefaultHttpResponse response() {
    return this.response;
  }
  
  /**
   * Returns the underlying Netty {@link Channel} of this query.
   */
  public Channel channel() {
    return chan;
  }

  /**
   * Returns the version for an API request. If the request was for a deprecated
   * API call (such as /q, /suggest, /logs) this value will be 0. If the request
   * was for a new API call, the version will be 1 or higher. If the user does
   * not supply a version, the MAX_API_VERSION value will be used.
   * @since 2.0
   */
  public int apiVersion() {
    return this.api_version;
  }
  
  /** @return Whether or not to show stack traces in errors @since 2.0 */
  public boolean showStackTrace() {
    return this.show_stack_trace;
  }
  
  /**
   * Return the {@link Deferred} associated with this query.
   */
  public Deferred<Object> getDeferred() {
    return deferred;
  }

  /** Returns how many ms have elapsed since this query was created. */
  public int processingTimeMillis() {
    return (int) ((System.nanoTime() - start_time) / 1000000);
  }

  /** @return The selected formatter. Will return null if {@link setFormatter}
   * hasn't been called yet @since 2.0  */
  public HttpFormatter formatter() {
    return this.formatter;
  }
  
  /**
   * Returns the query string parameters passed in the URI.
   */
  public Map<String, List<String>> getQueryString() {
    if (querystring == null) {
      try {
        querystring = new QueryStringDecoder(request.getUri()).getParameters();
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Bad query string: " + e.getMessage());
      }
    }
    return querystring;
  }

  /**
   * Returns the value of the given query string parameter.
   * <p>
   * If this parameter occurs multiple times in the URL, only the last value
   * is returned and others are silently ignored.
   * @param paramname Name of the query string parameter to get.
   * @return The value of the parameter or {@code null} if this parameter
   * wasn't passed in the URI.
   */
  public String getQueryStringParam(final String paramname) {
    final List<String> params = getQueryString().get(paramname);
    return params == null ? null : params.get(params.size() - 1);
  }

  /**
   * Returns the non-empty value of the given required query string parameter.
   * <p>
   * If this parameter occurs multiple times in the URL, only the last value
   * is returned and others are silently ignored.
   * @param paramname Name of the query string parameter to get.
   * @return The value of the parameter.
   * @throws BadRequestException if this query string parameter wasn't passed
   * or if its last occurrence had an empty value ({@code &amp;a=}).
   */
  public String getRequiredQueryStringParam(final String paramname)
    throws BadRequestException {
    final String value = getQueryStringParam(paramname);
    if (value == null || value.isEmpty()) {
      throw BadRequestException.missingParameter(paramname);
    }
    return value;
  }

  /**
   * Returns whether or not the given query string parameter was passed.
   * @param paramname Name of the query string parameter to get.
   * @return {@code true} if the parameter
   */
  public boolean hasQueryStringParam(final String paramname) {
    return getQueryString().get(paramname) != null;
  }

  /**
   * Returns all the values of the given query string parameter.
   * <p>
   * In case this parameter occurs multiple times in the URL, this method is
   * useful to get all the values.
   * @param paramname Name of the query string parameter to get.
   * @return The values of the parameter or {@code null} if this parameter
   * wasn't passed in the URI.
   */
  public List<String> getQueryStringParams(final String paramname) {
    return getQueryString().get(paramname);
  }

  /**
   * Returns only the path component of the URI as a string
   * This call strips the protocol, host, port and query string parameters 
   * leaving only the path e.g. "/path/starts/here"
   * <p>
   * Note that for slightly quicker performance you can call request().getUri()
   * to get the full path as a string but you'll have to strip query string
   * parameters manually.
   * @return The path component of the URI
   * @throws NullPointerException if the URI is null
   * @since 2.0
   */
  public String getQueryPath() {
    return new QueryStringDecoder(request.getUri()).getPath();
  }
  
  /**
   * Returns the path component of the URI as an array of strings, split on the
   * forward slash
   * Similar to the {@link getQueryPath} call, this returns only the path 
   * without the protocol, host, port or query string params. E.g. 
   * "/path/starts/here" will return an array of {"path", "starts", "here"}
   * <p>
   * Note that for maximum speed you may want to parse the query path manually.
   * @return An array with 1 or more components, note the first item may be
   * an empty string.
   * @throws BadRequestException if the URI is empty or does not start with a
   * slash
   * @throws NullPointerException if the URI is null
   * @since 2.0
   */
  public String[] explodePath() {
    final String path = this.getQueryPath();
    if (path.isEmpty()) {
      throw new BadRequestException("Query path is empty");
    }
    if (path.charAt(0) != '/') { 
      throw new BadRequestException("Query path doesn't start with a slash");
    }
    // split may be a tad slower than other methods, but since the URIs are
    // usually pretty short and not every request will make this call, we 
    // probably don't need any premature optimization
    return path.substring(1).split("/");
  }
  
  /**
   * Parses the query string to determine the base route for handing a query 
   * off to an RPC handler.
   * This method splits the query path component and returns a string suitable
   * for routing by {@see RpcHandler}. The resulting route is always lower case
   * and will consist of either an empty string, a deprecated API call or an
   * API route. API routes will set the {@link api_version} to either a user 
   * provided value or the MAX_API_VERSION.
   * <p>
   * Some URIs and their routes include:<ul>
   * <li>"/" - "" - the home directory</li>
   * <li>"/q?start=1h-ago&m=..." - "q" - a deprecated API call</li>
   * <li>"/api/v4/query" - "api/query" - a versioned API call</li>
   * <li>"/api/query" - "api/query" - a default versioned API call</li>
   * </ul>
   * @return the base route
   * @throws NumberFormatException if the version cannot be parsed
   * @since 2.0
   */
  public String getQueryBaseRoute() {
    final String[] split = this.explodePath();
    if (split.length < 1) {
      return "";
    }
    if (!split[0].toLowerCase().equals("api")) {
      return split[0].toLowerCase();
    }
    if (split.length < 2) {
      return "api";
    }
    if (split[1].toLowerCase().startsWith("v") && split[1].length() > 1 && 
        Character.isDigit(split[1].charAt(1))) {
      final int version = Integer.parseInt(split[1].substring(1));
      this.api_version = version > MAX_API_VERSION ? MAX_API_VERSION : version;
    } else {
      this.api_version = MAX_API_VERSION;
      return "api/" + split[1].toLowerCase();
    }
    if (split.length < 3){
      return "api";
    }
    return "api/" + split[2].toLowerCase();
  }
  
  /**
   * Attempts to parse the character set from the request header. If not set
   * defaults to UTF-8
   * @return A Charset object
   * @throws UnsupportedCharsetException if the parsed character set is invalid
   * @since 2.0
   */
  public Charset getCharset() {
    // RFC2616 3.7
    for (String type : this.request.getHeaders("Content-Type")) {
      int idx = type.toUpperCase().indexOf("CHARSET=");
      if (idx > 1) {
        String charset = type.substring(idx+8);
        return Charset.forName(charset);
      }
    }
    return Charset.forName("UTF-8");
  }
  
  /** @return True if the request has content, false if not @since 2.0 */
  public boolean hasContent() {
    return this.request.getContent() != null && 
      this.request.getContent().readable();
  }
  
  /**
   * Decodes the request content to a string using the appropriate character set
   * @return Decoded content or an empty string if the request did not include
   * content
   * @throws UnsupportedCharsetException if the parsed character set is invalid
   * @since 2.0
   */
  public String getContent() {
    return this.request.getContent().toString(this.getCharset());
  }
  
  /**
   * Sets the local formatter based on a query string parameter or content type.
   * <p>
   * If the caller supplies a "formatter=" parameter, the proper formatter is
   * loaded if found. If the formatter doesn't exist, an exception will be 
   * thrown, the JsonFormatter selected, and the user gets the error
   * <p>
   * If no query string parameter is supplied, the Content-Type header for the
   * request is parsed and if a matching formatter is found, it's used. 
   * Otherwise we default to the JsonFormatter.
   * @throws InvocationTargetException if the formatter cannot be instantiated
   * @throws IllegalArgumentException if the formatter cannot be instantiated
   * @throws InstantiationException if the formatter cannot be instantiated
   * @throws IllegalAccessException if a security manager is blocking access
   * @throws BadRequestException if a formatter requsted via query string does 
   * not exist
   */
  public void setFormatter() throws InvocationTargetException, 
    IllegalArgumentException, InstantiationException, IllegalAccessException {
    if (this.hasQueryStringParam("formatter")) {
      final String qs = this.getQueryStringParam("formatter");
      Constructor<? extends HttpFormatter> ctor = 
        formatter_map_query_string.get(qs);
      if (ctor == null) {
        this.formatter = new JsonFormatter(this);
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
            "Requested formatter was not found", 
            "Could not find a formatter with the name: " + qs);
      }
      
      this.formatter = ctor.newInstance(this);
      return;
    }
    
    // attempt to parse the Content-Type string. We only want the first part,
    // not the character set. And if the CT is missing, we'll use the default
    // formatter
    String content_type = this.request.getHeader("Content-Type");
    if (content_type == null || content_type.isEmpty()) {
      this.formatter = new JsonFormatter(this);
      return;
    }
    if (content_type.indexOf(";") > -1) {
      content_type = content_type.substring(0, content_type.indexOf(";"));
    }
    Constructor<? extends HttpFormatter> ctor = 
      formatter_map_content_type.get(content_type);
    if (ctor == null) {
      this.formatter = new JsonFormatter(this);
      return;
    }
    
    this.formatter = ctor.newInstance(this);
  }
  
  /**
   * Sends a 500 error page to the client.
   * Handles responses from deprecated API calls as well as newer, versioned
   * API calls
   * @param cause The unexpected exception that caused this error.
   */
  public void internalError(final Exception cause) {
    logError("Internal Server Error on " + request.getUri(), cause);
    
    if (this.api_version > 0) {
      if (this.formatter == null) { 
        this.formatter = new JsonFormatter(this);
      }
      switch (this.api_version) {
        case 1:
        default:
          sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
              formatter.formatErrorV1(cause));
      }
      return;
    }
    
    ThrowableProxy tp = new ThrowableProxy(cause);
    tp.calculatePackagingData();
    final String pretty_exc = ThrowableProxyUtil.asString(tp);
    tp = null;
    if (hasQueryStringParam("json")) {
      // 32 = 10 + some extra space as exceptions always have \t's to escape.
      final StringBuilder buf = new StringBuilder(32 + pretty_exc.length());
      buf.append("{\"err\":\"");
      HttpQuery.escapeJson(pretty_exc, buf);
      buf.append("\"}");
      sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, buf);
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.INTERNAL_SERVER_ERROR, pretty_exc, 30);
    } else {
      sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                makePage("Internal Server Error", "Houston, we have a problem",
                         "<blockquote>"
                         + "<h1>Internal Server Error</h1>"
                         + "Oops, sorry but your request failed due to a"
                         + " server error.<br/><br/>"
                         + "Please try again in 30 seconds.<pre>"
                         + pretty_exc
                         + "</pre></blockquote>"));
    }
  }

  /**
   * Sends a 400 error page to the client.
   * Handles responses from deprecated API calls as well as newer, versioned
   * API calls
   * @param explain The string describing why the request is bad.
   */
  public void badRequest(final BadRequestException exception) {
    logWarn("Bad Request on " + request.getUri() + ": " + exception.getMessage());
    if (this.api_version > 0) {
      switch (this.api_version) {
        case 1:
        default:
          sendReply(exception.getStatus(), formatter.formatErrorV1(exception));
      }
      return;
    }
    if (hasQueryStringParam("json")) {
      final StringBuilder buf = new StringBuilder(10 + 
          exception.getDetails().length());
      buf.append("{\"err\":\"");
      HttpQuery.escapeJson(exception.getMessage(), buf);
      buf.append("\"}");
      sendReply(HttpResponseStatus.BAD_REQUEST, buf);
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.BAD_REQUEST, exception.getMessage(), 3600);
    } else {
      sendReply(HttpResponseStatus.BAD_REQUEST,
                makePage("Bad Request", "Looks like it's your fault this time",
                         "<blockquote>"
                         + "<h1>Bad Request</h1>"
                         + "Sorry but your request was rejected as being"
                         + " invalid.<br/><br/>"
                         + "The reason provided was:<blockquote>"
                         + exception.getMessage()
                         + "</blockquote></blockquote>"));
    }
  }

  /** Sends a 404 error page to the client. */
  public void notFound() {
    logWarn("Not Found: " + request.getUri());
    if (this.api_version > 0) { 
      switch (this.api_version) {
        case 1:
        default:
          sendReply(HttpResponseStatus.NOT_FOUND, formatter.formatNotFoundV1());
      }
      return;
    }
    if (hasQueryStringParam("json")) {
      sendReply(HttpResponseStatus.NOT_FOUND,
                new StringBuilder("{\"err\":\"Page Not Found\"}"));
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.NOT_FOUND, "Page Not Found", 3600);
    } else {
      sendReply(HttpResponseStatus.NOT_FOUND, PAGE_NOT_FOUND);
    }
  }

  /** Redirects the client's browser to the given location.  */
  public void redirect(final String location) {
    // set the header AND a meta refresh just in case
    response.setHeader("Location", location);
    sendReply(HttpResponseStatus.OK,
      new StringBuilder(
          "<html></head><meta http-equiv=\"refresh\" content=\"0; url="
           + location + "\"></head></html>")
         .toString().getBytes(this.getCharset())
    );
  }

  /**
   * Escapes a string appropriately to be a valid in JSON.
   * Valid JSON strings are defined in RFC 4627, Section 2.5.
   * @param s The string to escape, which is assumed to be in .
   * @param buf The buffer into which to write the escaped string.
   */
  static void escapeJson(final String s, final StringBuilder buf) {
    final int length = s.length();
    int extra = 0;
    // First count how many extra chars we'll need, if any.
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':
        case '\\':
        case '\b':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
          extra++;
          continue;
      }
      if (c < 0x001F) {
        extra += 4;
      }
    }
    if (extra == 0) {
      buf.append(s);  // Nothing to escape.
      return;
    }
    buf.ensureCapacity(buf.length() + length + extra);
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':  buf.append('\\').append('"');  continue;
        case '\\': buf.append('\\').append('\\'); continue;
        case '\b': buf.append('\\').append('b');  continue;
        case '\f': buf.append('\\').append('f');  continue;
        case '\n': buf.append('\\').append('n');  continue;
        case '\r': buf.append('\\').append('r');  continue;
        case '\t': buf.append('\\').append('t');  continue;
      }
      if (c < 0x001F) {
        buf.append('\\').append('u').append('0').append('0')
          .append((char) Const.HEX[(c >>> 4) & 0x0F])
          .append((char) Const.HEX[c & 0x0F]);
      } else {
        buf.append(c);
      }
    }
  }

  /**
   * Sends data in an HTTP "200 OK" reply to the client.
   * @param data Raw byte array to send as-is after the HTTP headers.
   */
  public void sendReply(final byte[] data) {
    sendBuffer(HttpResponseStatus.OK, ChannelBuffers.wrappedBuffer(data));
  }
  
  /**
   * Sends data to the client with the given HTTP status code.
   * @param status HTTP status code to return
   * @param data Raw byte array to send as-is after the HTTP headers.
   * @since 2.0
   */
  public void sendReply(final HttpResponseStatus status, final byte[] data) {
    sendBuffer(status, ChannelBuffers.wrappedBuffer(data));
  }

  /**
   * Sends an HTTP reply to the client.
   * <p>
   * This is equivalent of
   * <code>{@link sendReply(HttpResponseStatus, StringBuilder)
   * sendReply}({@link HttpResponseStatus#OK
   * HttpResponseStatus.OK}, buf)</code>
   * @param buf The content of the reply to send.
   */
  public void sendReply(final StringBuilder buf) {
    sendReply(HttpResponseStatus.OK, buf);
  }

  /**
   * Sends an HTTP reply to the client.
   * <p>
   * This is equivalent of
   * <code>{@link sendReply(HttpResponseStatus, StringBuilder)
   * sendReply}({@link HttpResponseStatus#OK
   * HttpResponseStatus.OK}, buf)</code>
   * @param buf The content of the reply to send.
   */
  public void sendReply(final String buf) {
    sendBuffer(HttpResponseStatus.OK,
               ChannelBuffers.copiedBuffer(buf, CharsetUtil.UTF_8));
  }

  /**
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  public void sendReply(final HttpResponseStatus status,
                        final StringBuilder buf) {
    sendBuffer(status, ChannelBuffers.copiedBuffer(buf.toString(),
                                                   CharsetUtil.UTF_8));
  }

  /**
   * Sends the given message as a PNG image.
   * <strong>This method will block</strong> while image is being generated.
   * It's only recommended for cases where we want to report an error back to
   * the user and the user's browser expects a PNG image.  Don't abuse it.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param msg The message to send as an image.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendAsPNG(final HttpResponseStatus status,
                        final String msg,
                        final int max_age) {
    try {
      final long now = System.currentTimeMillis() / 1000;
      Plot plot = new Plot(now - 1, now);
      HashMap<String, String> params = new HashMap<String, String>(1);
      StringBuilder buf = new StringBuilder(1 + msg.length() + 18);

      buf.append('"');
      escapeJson(msg, buf);
      buf.append("\" at graph 0.02,0.97");
      params.put("label", buf.toString());
      buf = null;
      plot.setParams(params);
      params = null;
      final String basepath =
        tsdb.getConfig().getString("tsd.http.cachedir") 
        + Integer.toHexString(msg.hashCode());
      GraphHandler.runGnuplot(this, basepath, plot);
      plot = null;
      sendFile(status, basepath + ".png", max_age);
    } catch (Exception e) {
      getQueryString().remove("png");  // Avoid recursion.
      this.sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, 
          formatter.formatErrorV1(new RuntimeException(
              "Failed to generate a PNG with the"
              + " following message: " + msg, e)));
    }
  }

  /**
   * Send a file (with zero-copy) to the client with a 200 OK status.
   * This method doesn't provide any security guarantee.  The caller is
   * responsible for the argument they pass in.
   * @param path The path to the file to send to the client.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendFile(final String path,
                       final int max_age) throws IOException {
    sendFile(HttpResponseStatus.OK, path, max_age);
  }

  /**
   * Send a file (with zero-copy) to the client.
   * This method doesn't provide any security guarantee.  The caller is
   * responsible for the argument they pass in.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param path The path to the file to send to the client.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendFile(final HttpResponseStatus status,
                       final String path,
                       final int max_age) throws IOException {
    if (max_age < 0) {
      throw new IllegalArgumentException("Negative max_age=" + max_age
                                         + " for path=" + path);
    }
    if (!chan.isConnected()) {
      done();
      return;
    }
    RandomAccessFile file;
    try {
      file = new RandomAccessFile(path, "r");
    } catch (FileNotFoundException e) {
      logWarn("File not found: " + e.getMessage());
      if (querystring != null) {
        querystring.remove("png");  // Avoid potential recursion.
      }
      if (this.formatter == null) { 
        this.formatter = new JsonFormatter(this);
      }
      this.sendReply(HttpResponseStatus.NOT_FOUND, formatter.formatNotFoundV1());
      return;
    }
    final long length = file.length();
    {
      final String mimetype = guessMimeTypeFromUri(path);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
                         mimetype == null ? "text/plain" : mimetype);
      final long mtime = new File(path).lastModified();
      if (mtime > 0) {
        response.setHeader(HttpHeaders.Names.AGE,
                           (System.currentTimeMillis() - mtime) / 1000);
      } else {
        logWarn("Found a file with mtime=" + mtime + ": " + path);
      }
      response.setHeader(HttpHeaders.Names.CACHE_CONTROL,
                         max_age == 0 ? "no-cache" : "max-age=" + max_age);
      HttpHeaders.setContentLength(response, length);
      chan.write(response);
    }
    final DefaultFileRegion region = new DefaultFileRegion(file.getChannel(),
                                                           0, length);
    final ChannelFuture future = chan.write(region);
    future.addListener(new ChannelFutureListener() {
      public void operationComplete(final ChannelFuture future) {
        region.releaseExternalResources();
        done();
      }
    });
    if (!HttpHeaders.isKeepAlive(request)) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Method to call after writing the HTTP response to the wire.
   */
  private void done() {
    final int processing_time = processingTimeMillis();
    httplatency.add(processing_time);
    logInfo("HTTP " + request.getUri() + " done in " + processing_time + "ms");
    deferred.callback(null);
  }

  /**
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  private void sendBuffer(final HttpResponseStatus status,
                          final ChannelBuffer buf) {
    if (!chan.isConnected()) {
      done();
      return;
    }
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, 
        (formatter == null || api_version < 1 ? guessMimeType(buf) : 
          formatter.responseContentType()));
    
    // TODO(tsuna): Server, X-Backend, etc. headers.
    // only reset if we have the default status, otherwise the user set it
    if (response.getStatus() == HttpResponseStatus.ACCEPTED) {
      response.setStatus(status);
    }
    response.setContent(buf);
    final boolean keepalive = HttpHeaders.isKeepAlive(request);
    if (keepalive) {
      HttpHeaders.setContentLength(response, buf.readableBytes());
    }
    final ChannelFuture future = chan.write(response);
    if (!keepalive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
    done();
  }

  /**
   * Returns the result of an attempt to guess the MIME type of the response.
   * @param buf The content of the reply to send.
   */
  private String guessMimeType(final ChannelBuffer buf) {
    final String mimetype = guessMimeTypeFromUri(request.getUri());
    return mimetype == null ? guessMimeTypeFromContents(buf) : mimetype;
  }

  /**
   * Attempts to guess the MIME type by looking at the URI requested.
   * @param uri The URI from which to infer the MIME type.
   */
  private static String guessMimeTypeFromUri(final String uri) {
    final int questionmark = uri.indexOf('?', 1);  // 1 => skip the initial /
    final int end = (questionmark > 0 ? questionmark : uri.length()) - 1;
    if (end < 5) {  // Need at least: "/a.js"
      return null;
    }
    final char a = uri.charAt(end - 3);
    final char b = uri.charAt(end - 2);
    final char c = uri.charAt(end - 1);
    switch (uri.charAt(end)) {
      case 'g':
        return a == '.' && b == 'p' && c == 'n' ? "image/png" : null;
      case 'l':
        return a == 'h' && b == 't' && c == 'm' ? HTML_CONTENT_TYPE : null;
      case 's':
        if (a == '.' && b == 'c' && c == 's') {
          return "text/css";
        } else if (b == '.' && c == 'j') {
          return "text/javascript";
        } else {
          break;
        }
      case 'f':
        return a == '.' && b == 'g' && c == 'i' ? "image/gif" : null;
      case 'o':
        return a == '.' && b == 'i' && c == 'c' ? "image/x-icon" : null;
    }
    return null;
  }

  /**
   * Simple "content sniffing".
   * May not be a great idea, but will do until this class has a better API.
   * @param buf The content of the reply to send.
   * @return The MIME type guessed from {@code buf}.
   */
  private String guessMimeTypeFromContents(final ChannelBuffer buf) {
    if (!buf.readable()) {
      logWarn("Sending an empty result?! buf=" + buf);
      return "text/plain";
    }
    final int firstbyte = buf.getUnsignedByte(buf.readerIndex());
    switch (firstbyte) {
      case '<':  // <html or <!DOCTYPE
        return HTML_CONTENT_TYPE;
      case '{':  // JSON object
      case '[':  // JSON array
        return "application/json";  // RFC 4627 section 6 mandates this.
      case 0x89:  // magic number in PNG files.
        return "image/png";
    }
    return "text/plain";  // Default.
  }

  /**
   * Loads the formatter maps with present, implemented formatters. If no
   * plugins are loaded, only the default implementations will be available.
   * This method also builds the status map that users can access via the API
   * to see what has been implemented.
   * <p>
   * <b>WARNING:</b> The TSDB should have called on of the JAR load or search
   * methods from PluginLoader before calling this method. This will only scan
   * the class path for plugins that implement the HttpFormatter class
   * @param tsdb The TSDB to pass on to plugins
   * @throws NoSuchMethodException if a class could not be instantiated
   * @throws SecurityException if a security manager is present and causes
   * trouble
   * @throws ClassNotFoundException if the base class couldn't be found, for
   * some really odd reason
   * @throws IllegalArgumentException if a mapping collision occurs
   * @since 2.0
   */
  public static void InitializeFormatterMaps(final TSDB tsdb) 
    throws SecurityException, NoSuchMethodException, ClassNotFoundException {
    List<HttpFormatter> formatters = 
      PluginLoader.loadPlugins(HttpFormatter.class);
     
    // add the default formatters compiled with OpenTSDB
    if (formatters == null) { 
      formatters = new ArrayList<HttpFormatter>();
    }
    HttpFormatter default_formatter = new JsonFormatter();
    formatters.add(default_formatter);
     
    formatter_map_content_type = 
      new HashMap<String, Constructor<? extends HttpFormatter>>();
    formatter_map_query_string = 
      new HashMap<String, Constructor<? extends HttpFormatter>>();
    formatter_status = new ArrayList<HashMap<String, Object>>();
     
    for (HttpFormatter formatter : formatters) {
      Constructor<? extends HttpFormatter> ctor = 
        formatter.getClass().getDeclaredConstructor(HttpQuery.class);
      ctor.setAccessible(true);
       
      // check for collisions before adding formatters to the maps
      Constructor<? extends HttpFormatter> map_ctor = 
        formatter_map_content_type.get(formatter.requestContentType());
      if (map_ctor != null) {
        final String err = "Formatter content type collision between \"" + 
        formatter.getClass().getCanonicalName() + "\" and \"" + 
        map_ctor.getClass().getCanonicalName() + "\"";
        LOG.error(err);
        throw new IllegalArgumentException(err);
      } else { 
        formatter_map_content_type.put(formatter.requestContentType(), 
            ctor);
      }
      
      map_ctor = formatter_map_query_string.get(formatter.shortName());
      if (map_ctor != null) {
        final String err = "Formatter name collision between \"" + 
        formatter.getClass().getCanonicalName() + "\" and \"" + 
        map_ctor.getClass().getCanonicalName() + "\"";
        LOG.error(err);
        throw new IllegalArgumentException(err);
      } else { 
        formatter_map_query_string.put(formatter.shortName(), ctor);
      }
      
      // initialize the plugins
      formatter.initialize(tsdb);
      
      // write the status for any formatters OTHER than the default
      if (formatter.shortName().equals("json")) {
        continue;
      }
      HashMap<String, Object> status = new HashMap<String, Object>();
      status.put("version", formatter.version());
      status.put("class", formatter.getClass().getCanonicalName());
      status.put("formatter", formatter.shortName());
      status.put("request_content_type", formatter.requestContentType());
      status.put("response_content_type", formatter.responseContentType());
      
      ArrayList<String> parsers = new ArrayList<String>();
      ArrayList<String> formats = new ArrayList<String>();
      Method[] methods = formatter.getClass().getDeclaredMethods();
      for (Method m : methods) {
        if (Modifier.isPublic(m.getModifiers())) {
          if (m.getName().startsWith("parse")) {
            parsers.add(m.getName().substring(5));
          }
          if (m.getName().startsWith("format")) {
            formats.add(m.getName().substring(6));
          }
        }
      }
      status.put("parsers", parsers);
      status.put("formatters", formats);
      formatter_status.add(status);
    }
    
    // add the base class to the status map so users can see everything that
    // is implemented
    HashMap<String, Object> status = new HashMap<String, Object>();
    // todo - set the OpenTSDB version
    //status.put("version", BuildData.version);
    Class<?> base_formatter = Class.forName("net.opentsdb.tsd.HttpFormatter");
    status.put("class", default_formatter.getClass().getCanonicalName());
    status.put("formatter", default_formatter.shortName());
    status.put("request_content_type", default_formatter.requestContentType());
    status.put("response_content_type", default_formatter.responseContentType());
    
    ArrayList<String> parsers = new ArrayList<String>();
    ArrayList<String> formats = new ArrayList<String>();
    Method[] methods = base_formatter.getDeclaredMethods();
    for (Method m : methods) {
      if (Modifier.isPublic(m.getModifiers())) {
        if (m.getName().startsWith("parse")) {
          parsers.add(m.getName().substring(5));
        }
        if (m.getName().startsWith("format")) {
          formats.add(m.getName().substring(6));
        }
      }
    }
    status.put("parsers", parsers);
    status.put("formatters", formats);
    formatter_status.add(status);
  }
  
  /** @return the formatter status list and maps @since 2.0 */
  public static ArrayList<HashMap<String, Object>> getFormatterStatus() {
    return formatter_status;
  }

  /**
   * Easy way to generate a small, simple HTML page.
   * <p>
   * Equivalent to {@code makePage(null, title, subtitle, body)}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
  public static StringBuilder makePage(final String title,
                                       final String subtitle,
                                       final String body) {
    return makePage(null, title, subtitle, body);
  }

  /**
   * Easy way to generate a small, simple HTML page.
   * @param htmlheader Text to insert in the {@code head} tag.
   * Ignored if {@code null}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
  public static StringBuilder makePage(final String htmlheader,
                                       final String title,
                                       final String subtitle,
                                       final String body) {
    final StringBuilder buf = new StringBuilder(
      BOILERPLATE_LENGTH + (htmlheader == null ? 0 : htmlheader.length())
      + title.length() + subtitle.length() + body.length());
    buf.append(PAGE_HEADER_START)
      .append(title)
      .append(PAGE_HEADER_MID);
    if (htmlheader != null) {
      buf.append(htmlheader);
    }
    buf.append(PAGE_HEADER_END_BODY_START)
      .append(subtitle)
      .append(PAGE_BODY_MID)
      .append(body)
      .append(PAGE_FOOTER);
    return buf;
  }
  
  /** @return Information aboutt the query */
  public String toString() {
    return "HttpQuery"
      + "(start_time=" + start_time
      + ", request=" + request
      + ", chan=" + chan
      + ", querystring=" + querystring
      + ')';
  }
   
  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  private void logInfo(final String msg) {
    LOG.info(chan.toString() + ' ' + msg);
  }

  private void logWarn(final String msg) {
    LOG.warn(chan.toString() + ' ' + msg);
  }

  private void logError(final String msg, final Exception e) {
    LOG.error(chan.toString() + ' ' + msg, e);
  }
   
  // -------------------------------------------- //
  // Boilerplate (shamelessly stolen from Google) //
  // -------------------------------------------- //

  private static final String PAGE_HEADER_START =
    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">"
    + "<html><head>"
    + "<meta http-equiv=content-type content=\"text/html;charset=utf-8\">"
    + "<title>";

  private static final String PAGE_HEADER_MID =
    "</title>\n"
    + "<style><!--\n"
    + "body{font-family:arial,sans-serif;margin-left:2em}"
    + "A.l:link{color:#6f6f6f}"
    + "A.u:link{color:green}"
    + ".subg{background-color:#e2f4f7}"
    + ".fwf{font-family:monospace;white-space:pre-wrap}"
    + "//--></style>";

  private static final String PAGE_HEADER_END_BODY_START =
    "</head>\n"
    + "<body text=#000000 bgcolor=#ffffff>"
    + "<table border=0 cellpadding=2 cellspacing=0 width=100%>"
    + "<tr><td rowspan=3 width=1% nowrap><b>"
    + "<font color=#c71a32 size=10>T</font>"
    + "<font color=#00a189 size=10>S</font>"
    + "<font color=#1a65b7 size=10>D</font>"
    + "&nbsp;&nbsp;</b><td>&nbsp;</td></tr>"
    + "<tr><td class=subg><font color=#507e9b><b>";

  private static final String PAGE_BODY_MID =
    "</b></td></tr>"
    + "<tr><td>&nbsp;</td></tr></table>";

  private static final String PAGE_FOOTER =
    "<table width=100% cellpadding=0 cellspacing=0>"
    + "<tr><td class=subg><img alt=\"\" width=1 height=6></td></tr>"
    + "</table></body></html>";

  private static final int BOILERPLATE_LENGTH =
    PAGE_HEADER_START.length()
    + PAGE_HEADER_MID.length()
    + PAGE_HEADER_END_BODY_START.length()
    + PAGE_BODY_MID.length()
    + PAGE_FOOTER.length();

  /** Precomputed 404 page. */
  private static final StringBuilder PAGE_NOT_FOUND =
    makePage("Page Not Found", "Error 404",
             "<blockquote>"
             + "<h1>Page Not Found</h1>"
             + "The requested URL was not found on this server."
             + "</blockquote>");
}

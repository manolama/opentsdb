// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

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

/**
 * Binds together an HTTP request and the channel on which it was received.
 *
 * It makes it easier to provide a few utility methods to respond to the
 * requests.
 * 
 * The IO for the HTTP API is JSON but you can overload it with your own
 * data format if necessary.
 */
class HttpQuery {

  private static final Logger LOG = LoggerFactory.getLogger(HttpQuery.class);

  protected static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  /**
   * Keep track of the latency of HTTP requests.
   */
  protected static final Histogram httplatency =
    new Histogram(16000, (short) 2, 100);

  /** When the query was started (useful for timing). */
  protected final long start_time = System.nanoTime();

  /** The request in this HTTP query. */
  protected final HttpRequest request;

  /** The channel on which the request was received. */
  protected final Channel chan;

  /** Parsed query string (lazily built on first access). */
  protected Map<String, List<String>> querystring;

  /** Deferred result of this query, to allow asynchronous processing.  */
  protected final Deferred<Object> deferred = new Deferred<Object>();

  /** The {@code TSDB} instance we belong to */
  protected final TSDB tsdb;
  
  /** HTTP cache to store/read from */
  protected final HttpCache cache;
  
  protected String jsonp = "";
  
  protected String content_type = "";
  
  protected static final Charset CHARSET = Charset.forName("UTF-8");
  
  /**
   * Constructor.
   * @param request The request in this HTTP query.
   * @param chan The channel on which the request was received.
   */
  public HttpQuery(final TSDB tsdb, final HttpRequest request, 
      final Channel chan, final HttpCache cache) {
    this.tsdb = tsdb;
    this.request = request;
    this.chan = chan;
    this.cache = cache;
    this.jsonp = JSON_HTTP.getJsonPFunction(this);
    
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

  /**
   * Returns the underlying Netty {@link Channel} of this query.
   */
  public Channel channel() {
    return chan;
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

  public String getPostData(){
    return this.request().getContent().toString(CHARSET);
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
   * Returns a timestamp from a date specified in a query string parameter.
   * Formats accepted are:
   *   - Relative: "5m-ago", "1h-ago", etc.  See {@link #parseDuration}.
   *   - Absolute human readable date: "yyyy/MM/dd-HH:mm:ss".
   *   - UNIX timestamp (seconds since Epoch): "1234567890".
   * @param query The HTTP query from which to get the query string parameter.
   * @param paramname The name of the query string parameter.
   * @return A UNIX timestamp in seconds (strictly positive 32-bit "unsigned")
   * or -1 if there was no query string parameter named {@code paramname}.
   * @throws BadRequestException if the date is invalid.
   */
  public long getQueryStringDate(final String paramname) {
    final String date = getQueryStringParam(paramname);
    if (date == null) {
      return -1;
    } else if (date.endsWith("-ago")) {
      return (System.currentTimeMillis() / 1000
              - parseDuration(date.substring(0, date.length() - 4)));
    }
    long timestamp;
    try {
      timestamp = Long.parseLong(date);   // Is it already a timestamp?
    } catch (NumberFormatException ne) {  // Nope, try to parse a date then.
      try {
        final SimpleDateFormat fmt = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
        timestamp = fmt.parse(date).getTime() / 1000;
      } catch (ParseException e) {
        throw new BadRequestException("Invalid " + paramname + " date: " + date
                                      + ". " + e.getMessage());
      } catch (NumberFormatException e) {
        throw new BadRequestException("Invalid " + paramname + " date: " + date
                                      + ". " + e.getMessage());
      }
    }
    if (timestamp < 0) {
      throw new BadRequestException("Bad " + paramname + " date: " + date);
    }
    return timestamp;
  }
  
  /**
   * Parses a human-readable duration (e.g, "10m", "3h", "14d") into seconds.
   * <p>
   * Formats supported: {@code s}: seconds, {@code m}: minutes,
   * {@code h}: hours, {@code d}: days, {@code w}: weeks, {@code y}: years.
   * @param duration The human-readable duration to parse.
   * @return A strictly positive number of seconds.
   * @throws BadRequestException if the interval was malformed.
   */
  public static final int parseDuration(final String duration) {
    int interval;
    final int lastchar = duration.length() - 1;
    try {
      interval = Integer.parseInt(duration.substring(0, lastchar));
    } catch (NumberFormatException e) {
      throw new BadRequestException("Invalid duration (number): " + duration);
    }
    if (interval <= 0) {
      throw new BadRequestException("Zero or negative duration: " + duration);
    }
    switch (duration.charAt(lastchar)) {
      case 's': return interval;                    // seconds
      case 'm': return interval * 60;               // minutes
      case 'h': return interval * 3600;             // hours
      case 'd': return interval * 3600 * 24;        // days
      case 'w': return interval * 3600 * 24 * 7;    // weeks
      case 'y': return interval * 3600 * 24 * 365;  // years (screw leap years)
    }
    throw new BadRequestException("Invalid duration (suffix): " + duration);
  }
  
  /**
   * Decides how long we're going to allow the client to cache our response.
   * <p>
   * Based on the query, we'll decide whether or not we want to allow the
   * client to cache our response and for how long.
   * @param query The query to serve.
   * @param start_time The start time on the query (32-bit unsigned int, secs).
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param now The current time (32-bit unsigned int, seconds).
   * @return A positive integer, in seconds.
   */
  public int computeMaxAge(final long start_time, final long end_time, final long now) {
    // If the end time is in the future (1), make the graph uncacheable.
    // Otherwise, if the end time is far enough in the past (2) such that
    // no TSD can still be writing to rows for that time span and it's not
    // specified in a relative fashion (3) (e.g. "1d-ago"), make the graph
    // cacheable for a day since it's very unlikely that any data will change
    // for this time span.
    // Otherwise (4), allow the client to cache the graph for ~0.1% of the
    // time span covered by the request e.g., for 1h of data, it's OK to
    // serve something 3s stale, for 1d of data, 84s stale.
    if (end_time > now) {                            // (1)
      return 0;
    } else if (end_time < now - Const.MAX_TIMESPAN   // (2)
               && !isRelativeDate(this, "start")    // (3)
               && !isRelativeDate(this, "end")) {
      return 86400;
    } else {                                         // (4)
      return (int) (end_time - start_time) >> 10;
    }
  }
  
  /**
   * Returns whether or not a date is specified in a relative fashion.
   * <p>
   * A date is specified in a relative fashion if it ends in "-ago",
   * e.g. "1d-ago" is the same as "24h-ago".
   * @param query The HTTP query from which to get the query string parameter.
   * @param paramname The name of the query string parameter.
   * @return {@code true} if the parameter is passed and is a relative date.
   * Note the method doesn't attempt to validate the relative date.  So this
   * function can return true on something that looks like a relative date,
   * but is actually invalid once we really try to parse it.
   */
  private static boolean isRelativeDate(final HttpQuery query,
                                        final String paramname) {
    final String date = query.getQueryStringParam(paramname);
    return date == null || date.endsWith("-ago");
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
   * Sends a 500 error page to the client.
   * @param cause The unexpected exception that caused this error.
   */
  public void internalError(final Exception cause) {
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
    logError("Internal Server Error on " + request.getUri(), cause);
  }

  /**
   * Sends a 400 error page to the client.
   * @param explain The string describing why the request is bad.
   */
  public void badRequest(final String explain) {
    if (JSON_HTTP.getJsonRequested(this)) {
    	sendReply(new JsonRpcError(explain, 404).getJSON());
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.BAD_REQUEST, explain, 3600);
    } else {
      sendReply(HttpResponseStatus.BAD_REQUEST,
                makePage("Bad Request", "Looks like it's your fault this time",
                         "<blockquote>"
                         + "<h1>Bad Request</h1>"
                         + "Sorry but your request was rejected as being"
                         + " invalid.<br/><br/>"
                         + "The reason provided was:<blockquote>"
                         + explain
                         + "</blockquote></blockquote>"));
    }
    logWarn("Bad Request on " + request.getUri() + ": " + explain);
  }

  /**
   * Returns a hashcode identifying the query string and URI, 
   * stripping out any parameters that shouldn't affect cache
   * generation
   * @return An integer hash value (can be positive or negative)
   */
  public final int getQueryStringHash() {
    // copy the query string map first so we can tweak it
    final HashMap<String, List<String>> qs =
      new HashMap<String, List<String>>(this.querystring);
    // But first remove the parameters that don't influence the output.
    qs.remove("ignore");
    qs.remove("meta");
    // run this through "Integer.toHexString()" to get a string value
    return this.request.getUri().hashCode() | qs.hashCode();
  }
  
  /** Sends a 404 error page to the client. */
  public void notFound() {
    logWarn("Not Found: " + request.getUri());
    if (hasQueryStringParam("json")) {
      sendReply(HttpResponseStatus.NOT_FOUND,
                new StringBuilder("{\"err\":\"Page Not Found\"}"));
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.NOT_FOUND, "Page Not Found", 3600);
    } else {
      sendReply(HttpResponseStatus.NOT_FOUND, PAGE_NOT_FOUND);
    }
  }

  /**
   * Accessor for the cache object
   * @return Cache reference
   */
  public HttpCache getCache(){
    return this.cache;
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
               ChannelBuffers.copiedBuffer(buf, CHARSET));
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
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  public void sendReply(final HttpResponseStatus status, final String buf) {
	  sendBuffer(status, ChannelBuffers.copiedBuffer(buf, CharsetUtil.UTF_8));
  }

  public void sendError(final HttpResponseStatus status, final String buf) {
    final JsonRpcError jerror = new JsonRpcError(buf, status.getCode());
    this.content_type = "application/json";
    sendBuffer(status, ChannelBuffers.copiedBuffer(
        (this.jsonp.isEmpty() ? jerror.getJSON() : jerror.getJSONP(this.jsonp)), 
        CHARSET));
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
      
      // set the base path for the plot file
      String basepath = tsdb.getConfig().cacheDirectory();      
      // check for slashes
      if (System.getProperty("os.name").contains("Windows") 
          && !basepath.endsWith("\\")){
        basepath += "\\";
      }else if (!basepath.endsWith("/")){
        basepath += "/";
      }
      basepath += Integer.toHexString(msg.hashCode());
      
      //GraphHandler.runGnuplot(this, basepath, plot);
      plot = null;
      sendFile(status, basepath + ".png", max_age);
    } catch (Exception e) {
      getQueryString().remove("png");  // Avoid recursion.
      internalError(new RuntimeException("Failed to generate a PNG with the"
                                         + " following message: " + msg, e));
    }
  }

  /**
   * Checks whether or not it's possible to re-serve this query from disk.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.
   * @param basepath The base path used for the Gnuplot files.
   * @return {@code true} if this request was served from disk (in which
   * case processing can stop here), {@code false} otherwise (in which case
   * the query needs to be processed).
   */
  public boolean isDiskCacheHit(final long end_time,
                                 final int max_age,
                                 final String basepath) throws IOException {
    final String cachepath = basepath + (this.hasQueryStringParam("ascii")
                                         ? ".txt" : ".png");
    final File cachedfile = new File(cachepath);
    if (cachedfile.exists()) {
      final long bytes = cachedfile.length();
      if (bytes < 21) {  // Minimum possible size for a PNG: 21 bytes.
                         // For .txt files, <21 bytes is almost impossible.
        logWarn("Cached " + cachepath + " is too small ("
                + bytes + " bytes) to be valid.  Ignoring it.");
        return false;
      }
      if (staleCacheFile(end_time, max_age, cachedfile)) {
        return false;
      }
      if (this.hasQueryStringParam("json")) {
        StringBuilder json = loadCachedJson(this, end_time, max_age, basepath);
        if (json == null) {
          json = new StringBuilder(32);
          json.append("{\"timing\":");
        }
        json.append(this.processingTimeMillis())
          .append(",\"cachehit\":\"disk\"}");
        this.sendReply(json);
      } else if (this.hasQueryStringParam("png")
                 || this.hasQueryStringParam("ascii")) {
        this.sendFile(cachepath, max_age);
      } else {
        this.sendReply(HttpQuery.makePage("TSDB Query", "Your graph is ready",
            "<img src=\"" + this.request().getUri() + "&amp;png\"/><br/>"
            + "<small>(served from disk cache)</small>"));
      }
      //graphs_diskcache_hit.incrementAndGet();
      return true;
    }
    
    // We didn't find an image.  Do a negative cache check.  If we've seen
    // this query before but there was no result, we at least wrote the JSON.
    final StringBuilder json = loadCachedJson(this, end_time, max_age, basepath);
    // If we don't have a JSON file it's a complete cache miss.  If we have
    // one, and it says 0 data points were plotted, it's a negative cache hit.
    if (json == null || !json.toString().contains("\"plotted\":0")) {
      return false;
    }
    if (this.hasQueryStringParam("json")) {
      json.append(this.processingTimeMillis())
        .append(",\"cachehit\":\"disk\"}");
      this.sendReply(json);
    } else if (this.hasQueryStringParam("png")) {
      this.sendReply(" ");  // Send back an empty response...
    } else {
      this.sendReply(HttpQuery.makePage("TSDB Query", "No results",
            "Sorry, your query didn't return anything.<br/>"
            + "<small>(served from disk cache)</small>"));
    }
    //graphs_diskcache_hit.incrementAndGet();
    return true;
  }
  
  /**
   * Attempts to read the cached {@code .json} file for this query.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.
   * @param basepath The base path used for the Gnuplot files.
   * @return {@code null} in case no file was found, or the contents of the
   * file if it was found.  In case some contents was found, it is truncated
   * after the position of the last `:' in order to allow the caller to add
   * the time taken to serve by the request and other JSON elements if wanted.
   */
  private StringBuilder loadCachedJson(final HttpQuery query,
                                       final long end_time,
                                       final long max_age,
                                       final String basepath) {
    final String json_path = basepath + ".json";
    File json_cache = new File(json_path);
    if (staleCacheFile(end_time, max_age, json_cache)) {
      return null;
    }
    final byte[] json = readFile(json_cache, 4096);
    if (json == null) {
      return null;
    }
    json_cache = null;
    final StringBuilder buf = new StringBuilder(20 + json.length);
    // The json file is always expected to end in: {...,"timing":N}
    // We remove everything past the last `:' so we can send the new
    // timing for this request.  This doesn't work if there's a tag name
    // with a `:' in it, which is not allowed right now.
    int colon = 0;  // 0 isn't a valid value.
    for (int i = 0; i < json.length; i++) {
      buf.append((char) json[i]);
      if (json[i] == ':') {
        colon = i;
      }
    }
    if (colon != 0) {
      buf.setLength(colon + 1);
      return buf;
    } else {
      logError("No `:' found in " + json_path + " (" + json.length
               + " bytes) = " + new String(json));
    }
    return null;
  }
  
  /**
   * Reads a file into a byte array.
   * @param query The query being handled (for logging purposes).
   * @param file The file to read.
   * @param max_length The maximum number of bytes to read from the file.
   * @return {@code null} if the file doesn't exist or is empty or couldn't be
   * read, otherwise a byte array of up to {@code max_length} bytes.
   */
  public static byte[] readFile(final File file,
                                 final int max_length) {
    final int length = (int) file.length();
    if (length <= 0) {
      return null;
    }
    FileInputStream in;
    try {
      in = new FileInputStream(file.getPath());
    } catch (FileNotFoundException e) {
      return null;
    }
    try {
      final byte[] buf = new byte[Math.min(length, max_length)];
      final int read = in.read(buf);
      if (read != buf.length) {
        LOG.error("When reading " + file + ": read only "
                 + read + " bytes instead of " + buf.length);
        return null;
      }
      return buf;
    } catch (IOException e) {
      LOG.error("Error while reading " + file, e);
      return null;
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.error("Error while closing " + file, e);
      }
    }
  }
  
  /**
   * Returns whether or not the given cache file can be used or is stale.
   * @param query The query to serve.
   * @param end_time The end time on the query (32-bit unsigned int, seconds).
   * @param max_age The maximum time (in seconds) we wanna allow clients to
   * cache the result in case of a cache hit.  If the file is exactly that
   * old, it is not considered stale.
   * @param cachedfile The file to check for staleness.
   */
  private boolean staleCacheFile(final long end_time,
                                        final long max_age,
                                        final File cachedfile) {
    final long mtime = cachedfile.lastModified() / 1000;
    if (mtime <= 0) {
      return true;  // File doesn't exist, or can't be read.
    }

    final long now = System.currentTimeMillis() / 1000;
    // How old is the cached file, in seconds?
    final long staleness = now - mtime;
    if (staleness < 0) {  // Can happen if the mtime is "in the future".
      logWarn("Not using file @ " + cachedfile + " with weird"
              + " mtime in the future: " + mtime);
      return true;  // Play it safe, pretend we can't use this file.
    }

    // Case 1: The end time is an absolute point in the past.
    // We might be able to re-use the cached file.
    if (0 < end_time && end_time < now) {
      // If the file was created prior to the end time, maybe we first
      // executed this query while the result was uncacheable.  We can
      // tell by looking at the mtime on the file.  If the file was created
      // before the query end time, then it contains partial results that
      // shouldn't be served again.
      return mtime < end_time;
    }

    // Case 2: The end time of the query is now or in the future.
    // The cached file contains partial data and can only be re-used if it's
    // not too old.
    if (staleness > max_age) {
      logInfo("Cached file @ " + cachedfile.getPath() + " is "
              + staleness + "s stale, which is more than its limit of "
              + max_age + "s, and needs to be regenerated.");
      return true;
    }
    return false;
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
      notFound();
      return;
    }
    final long length = file.length();
    {
      final DefaultHttpResponse response =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
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
    final DefaultHttpResponse response =
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    if (this.content_type.length() < 1)
      this.content_type = guessMimeType(buf);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, this.content_type);
    // TODO(tsuna): Server, X-Backend, etc. headers.
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

  public String toString() {
    return "HttpQuery"
      + "(start_time=" + start_time
      + ", request=" + request
      + ", chan=" + chan
      + ", querystring=" + querystring
      + ')';
  }

  // examines the method type AND the query string
  public HttpMethod getMethod(){
    if (this.request().getMethod().compareTo(HttpMethod.GET) == 0){
      if (hasQueryStringParam("method")){
        final String method = this.getQueryStringParam("method");
        if (method.toUpperCase().compareTo("GET") == 0)
          return this.request().getMethod();
        else if (method.toUpperCase().compareTo("POST") == 0)
          return HttpMethod.POST;
        else if (method.toUpperCase().compareTo("DELETE") == 0)
          return HttpMethod.DELETE;
        else if (method.toUpperCase().compareTo("PUT") == 0)
          return HttpMethod.PUT;
        else
          return HttpMethod.GET;
      }
      return HttpMethod.GET;
    }else
      return this.request().getMethod();
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
  
  private void logError(final String msg) {
    LOG.error(chan.toString() + ' ' + msg);
  }

  // -------------------------------------------- //
  // Boilerplate (shamelessly stolen from Google) //
  // -------------------------------------------- //

  private static final String PAGE_HEADER_START =
    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n"
    + "<html>\n<head>\n"
    + "\t<meta http-equiv=content-type content=\"text/html;charset=utf-8\">\n"
    + "\t<title>\n\n";

  private static final String PAGE_HEADER_MID =
    "\t</title>\n"
    + "\t\t<style><!--\n"
    + "body{font-family:arial,sans-serif;margin-left:2em}"
    + "A.l:link{color:#6f6f6f}"
    + "A.u:link{color:green}"
    + ".subg{background-color:#e2f4f7}"
    + ".fwf{font-family:monospace;white-space:pre-wrap}"
    + "//--></style>\n";

  private static final String PAGE_HEADER_END_BODY_START =
    "</head>\n"
    + "<body text=#000000 bgcolor=#ffffff>\n"
    + "\t<table border=0 cellpadding=2 cellspacing=0 width=100%>\n"
    + "\t\t<tr><td rowspan=3 width=1% nowrap><b>"
    + "<font color=#c71a32 size=10>T</font>"
    + "<font color=#00a189 size=10>S</font>"
    + "<font color=#1a65b7 size=10>D</font>"
    + "&nbsp;&nbsp;</b><td>&nbsp;</td></tr>\n"
    + "\t\t<tr><td class=subg><font color=#507e9b><b>";

  private static final String PAGE_BODY_MID =
    "</b></td></tr>\n"
    + "\t\t<tr><td>&nbsp;</td></tr>\n\t</table>\n";

  private static final String PAGE_FOOTER =
    "\t<table width=100% cellpadding=0 cellspacing=0>\n"
    + "\t<tr><td class=subg><img alt=\"\" width=1 height=6></td></tr>\n"
    + "\t</table>\n</body>\n</html>";

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

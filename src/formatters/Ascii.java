package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.BuildData;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.StatsCollector.StatsDP;
import net.opentsdb.tsd.DataQuery;
import net.opentsdb.tsd.HttpQuery;
import net.opentsdb.uid.NoSuchUniqueName;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

public class Ascii extends TSDFormatter{
//overload the log factory
  private static final Logger LOG = LoggerFactory.getLogger(Ascii.class);
 
  public Ascii(final TSDB tsdb){
    super(tsdb);
  }
  
  public String getEndpoint(){
    return "ascii";
  }
  
  public boolean validateQuery(final DataQuery dataquery){
    return true;
  }
  
  /**
   * Returns rows of complete data points in the format:
   * <metric> <timestamp> <value> <tag pairs>
   * @param query The HTTPQuery to parse
   * @return Returns true
   */
  public boolean handleHTTPDataGet(final HttpQuery query){  
    StringBuilder output = new StringBuilder(this.datapoints.size() * 1024);

    for (DataPoints dp : this.datapoints){
      
      // build the tags once for speed
      StringBuilder tags = new StringBuilder(dp.getTags().size() * 256);
      int counter=0;
      for (Map.Entry<String, String> pair : dp.getTags().entrySet()){
        if (counter > 0)
          tags.append(" ");
        tags.append(pair.getKey()).append("=").append(pair.getValue());
        counter++;
      }
      // now build the individual rows
      for(int i=0; i<dp.size(); i++){
        output.append(dp.metricName()).append(" ");
        output.append(dp.timestamp(i)).append(" ");
        output.append(dp.isInteger(i) ? dp.longValue(i) : dp.doubleValue(i)).append(" ");
        output.append(tags);
        output.append("\n");
      }
    }
    
    query.sendReply(output.toString());    
    return true;
  }
  
  public boolean handleHTTPFormatters(final HttpQuery query, 
      final HashMap<String, ArrayList<String>> formatters){
    StringBuffer buf = new StringBuffer(1024);
    for (Map.Entry<String, ArrayList<String>> entry : formatters.entrySet()){
      buf.append(entry.getKey()).append("\n");
      for (String method : entry.getValue())
        buf.append("  ").append(method).append("\n");
    }
    query.sendReply(buf.toString());
    return true;
  }
  
  public Deferred<Object> handleTelnetDataPut(String[] command, final Channel chan){
    String errmsg = null;
    try {
      final class PutErrback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          if (chan.isConnected()) {
            chan.write("put: HBase error: " + arg.getMessage() + '\n');
          }
          storage_errors.incrementAndGet();
          return arg;
        }

        public String toString() {
          return "report error to channel";
        }
      }
      return importDataPoint(tsdb, command).addErrback(new PutErrback());
    } catch (NumberFormatException x) {
      errmsg = "put: invalid value: " + x.getMessage() + '\n';
      invalid_values.incrementAndGet();
    } catch (IllegalArgumentException x) {
      errmsg = "put: illegal argument: " + x.getMessage() + '\n';
      illegal_arguments.incrementAndGet();
    } catch (NoSuchUniqueName x) {
      errmsg = "put: unknown metric: " + x.getMessage() + '\n';
      unknown_metrics.incrementAndGet();
    }
    if (errmsg != null && chan.isConnected()) {
      chan.write(errmsg);
    }
    return Deferred.fromResult(null);
  }

  public Deferred<Object> handleTelnetStats(String[] command, final Channel chan, 
      ArrayList<StatsDP> stats){
    final StringBuilder buf = new StringBuilder(1024);
    for (StatsDP stat : stats)
      buf.append(StatsCollector.getAscii(stat));
    chan.write(buf.toString());
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetVersion(String[] command, final Channel chan, 
      final HashMap<String, Object> version){
    if (chan.isConnected()) {
      chan.write(BuildData.revisionString() + '\n' + BuildData.buildString()
          + '\n');
    }
    return Deferred.fromResult(null);
  }
  
// PRIVATES ------------------------------------------------
  
  /**
   * Imports a single data point.
   * @param tsdb The TSDB to import the data point into.
   * @param words The words describing the data point to import, in the
   *          following format: {@code [metric, timestamp, value, ..tags..]}
   * @return A deferred object that indicates the completion of the request.
   * @throws NumberFormatException if the timestamp or value is invalid.
   * @throws IllegalArgumentException if any other argument is invalid.
   * @throws NoSuchUniqueName if the metric isn't registered.
   */
  private Deferred<Object> importDataPoint(final TSDB tsdb, final String[] words) {
    words[0] = null; // Ditch the "put".
    if (words.length < 5) { // Need at least: metric timestamp value tag
      // ^ 5 and not 4 because words[0] is "put".
      throw new IllegalArgumentException("not enough arguments"
          + " (need least 4, got " + (words.length - 1) + ')');
    }
    final String metric = words[1];
    if (metric.length() <= 0) {
      throw new IllegalArgumentException("empty metric name");
    }
    final long timestamp = Tags.parseLong(words[2]);
    if (timestamp <= 0) {
      throw new IllegalArgumentException("invalid timestamp: " + timestamp);
    }
    final String value = words[3];
    if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value");
    }
    final HashMap<String, String> tags = new HashMap<String, String>();
    for (int i = 4; i < words.length; i++) {
      if (!words[i].isEmpty()) {
        Tags.parse(tags, words[i]);
      }
    }
    if (value.indexOf('.') < 0) { // integer value
      return tsdb.addPoint(metric, timestamp, Tags.parseLong(value), tags);
    } else { // floating point value
      return tsdb.addPoint(metric, timestamp, Float.parseFloat(value), tags);
    }
  }
}

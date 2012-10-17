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
package net.opentsdb.stats;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Receives various stats/metrics from the current process.
 * <p>
 * Instances of this class are passed around to other classes to collect
 * their stats/metrics and do something with them (presumably send them
 * to a client).
 * <p>
 * This class does not do any synchronization and is not thread-safe.
 */
public abstract class StatsCollector {

  private static final Logger LOG =
    LoggerFactory.getLogger(StatsCollector.class);

  /** Prefix to add to every metric name, for example `tsd'. */
  private final String prefix;

  /** Extra tags to add to every data point emitted. */
  private HashMap<String, String> extratags;

//  /** Buffer used to build lines emitted. */
//  //private final StringBuilder buf = new StringBuilder();
//  private final List<StatsDP> datapoints = new ArrayList<StatsDP>();

  /**
   * Constructor.
   * @param prefix A prefix to add to every metric name, for example
   * `tsd'.
   */
  public StatsCollector(final String prefix) {
    this.prefix = prefix;
  }

  public void emit(StatsDP object){
    LOG.warn("Not implemented");
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   */
//  public final void record(final String name, final long value) {
//    record(name, value, null);
//  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   */
  public final void record(final String name, final Number value) {
    record(name, value, null);
  }

  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   * @param xtratag An extra tag ({@code name=value}) to add to those
   * data points (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
//  public final void record(final String name,
//                           final Number value,
//                           final Map<String, String> tags) {
//    record(name, value.longValue(), tags);
//  }
  
  public final void record(final String name,
                           final Number value,
                           final SimpleEntry<String, String> tag){
    Map<String, String> tags = new HashMap<String, String>();
    if (tag != null)
      tags.put(tag.getKey(), tag.getValue());
    record(name, tags, value.longValue());
  }

  /**
   * Records a number of data points from a {@link Histogram}.
   * @param name The name of the metric.
   * @param histo The histogram to collect data points from.
   * @param xtratag An extra tag ({@code name=value}) to add to those
   * data points (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
  public final void record(final String name,
                           final Histogram histo,
                           final Map<String, String> tags) {
    record(name + "_50pct", tags, histo.percentile(50));
    record(name + "_75pct", tags, histo.percentile(75));
    record(name + "_90pct", tags, histo.percentile(90));
    record(name + "_95pct", tags, histo.percentile(95));
  }
  
  public final void record(final String name,
      final Histogram histo,
      final SimpleEntry<String, String> tag){
    Map<String, String> tags = new HashMap<String, String>();
    tags.put(tag.getKey(), tag.getValue());
    record(name + "_50pct", tags, histo.percentile(50));
    record(name + "_75pct", tags, histo.percentile(75));
    record(name + "_90pct", tags, histo.percentile(90));
    record(name + "_95pct", tags, histo.percentile(95));
  }

  public final void record(final String name,
      final DescriptiveStatistics histo,
      final SimpleEntry<String, String> tag){
    Map<String, String> tags = new HashMap<String, String>();
    tags.put(tag.getKey(), tag.getValue());
    record(name + "_50pct", tags, histo.getPercentile(50));
    record(name + "_75pct", tags, histo.getPercentile(75));
    record(name + "_90pct", tags, histo.getPercentile(90));
    record(name + "_95pct", tags, histo.getPercentile(95));
    record(name + "_mean", tags, histo.getMean());
    record(name + "_max", tags, histo.getMax());
    record(name + "_min", tags, histo.getMin());
  }
  
  /**
   * Records a data point.
   * @param name The name of the metric.
   * @param value The current value for that metric.
   * @param xtratag An extra tag ({@code name=value}) to add to this
   * data point (ignored if {@code null}).
   * @throws IllegalArgumentException if {@code xtratag != null} and it
   * doesn't follow the {@code name=value} format.
   */
  private final void record(final String name,
      final Map<String, String> tags,
                           final Number value
                           ) {
//    buf.setLength(0);
//    buf.append(prefix).append(".")
//       .append(name)
//       .append(' ')
//       .append(System.currentTimeMillis() / 1000)
//       .append(' ')
//       .append(value);
    
    StatsDP dp = new StatsDP();
    dp.metric = (prefix.isEmpty() ? name : prefix + "." + name);
    dp.timestamp = System.currentTimeMillis() / 1000L;
    dp.value = value;
    dp.tags = tags;
    
    if (extratags != null){
      for (final Map.Entry<String, String> entry : extratags.entrySet()){
        if (dp.tags == null)
          dp.tags = new HashMap<String, String>();
        dp.tags.put(entry.getKey(), entry.getValue());
      }
    }
    
    //this.datapoints.add(dp);
//    if (xtratag != null) {
//      if (xtratag.indexOf('=') != xtratag.lastIndexOf('=')) {
//        throw new IllegalArgumentException("invalid xtratag: " + xtratag
//            + " (multiple '=' signs), name=" + name + ", value=" + value);
//      } else if (xtratag.indexOf('=') < 0) {
//        throw new IllegalArgumentException("invalid xtratag: " + xtratag
//            + " (missing '=' signs), name=" + name + ", value=" + value);
//      }
//      buf.append(' ').append(xtratag);
//    }
//
//    if (extratags != null) {
//      for (final Map.Entry<String, String> entry : extratags.entrySet()) {
//        buf.append(' ').append(entry.getKey())
//           .append('=').append(entry.getValue());
//      }
//    }
//    buf.append('\n');
    emit(dp);
  }

  /**
   * Adds a tag to all the subsequent data points recorded.
   * <p>
   * All subsequent calls to one of the {@code record} methods will
   * associate the tag given to this method with the data point.
   * <p>
   * This method can be called multiple times to associate multiple tags
   * with all the subsequent data points.
   * @param name The name of the tag.
   * @param value The value of the tag.
   * @throws IllegalArgumentException if the name or the value are empty
   * or otherwise invalid.
   * @see #clearExtraTag
   */
  public final void addExtraTag(final String name, final String value) {
    if (name.length() <= 0) {
      throw new IllegalArgumentException("empty tag name, value=" + value);
    } else if (value.length() <= 0) {
      throw new IllegalArgumentException("empty value, tag name=" + name);
    } else if (name.indexOf('=') != -1) {
      throw new IllegalArgumentException("tag name contains `=': " + name
                                         + " (value = " + value + ')');
    } else if (value.indexOf('=') != -1) {
      throw new IllegalArgumentException("tag value contains `=': " + value
                                         + " (name = " + name + ')');
    }
    if (extratags == null) {
      extratags = new HashMap<String, String>();
    }
    extratags.put(name, value);
  }

  /**
   * Adds a {@code host=hostname} tag.
   * <p>
   * This uses {@link InetAddress#getLocalHost} to find the hostname of the
   * current host.  If the hostname cannot be looked up, {@code (unknown)}
   * is used instead.
   */
  public final void addHostTag() {
    try {
      addExtraTag("host", InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException x) {
      LOG.error("WTF?  Can't find hostname for localhost!", x);
      addExtraTag("host", "(unknown)");
    }
  }

  /**
   * Clears a tag added using {@link #addExtraTag addExtraTag}.
   * @param name The name of the tag to remove from the set of extra
   * tags.
   * @throws IllegalStateException if there's no extra tag currently
   * recorded.
   * @throws IllegalArgumentException if the given name isn't in the
   * set of extra tags currently recorded.
   * @see #addExtraTag
   */
  public final void clearExtraTag(final String name) {
    if (extratags == null) {
      throw new IllegalStateException("no extra tags added");
    }
    if (extratags.get(name) == null) {
      throw new IllegalArgumentException("tag '" + name
          + "' not in" + extratags);
    }
    extratags.remove(name);
  }

  public static final String getAscii(final StatsDP dp){
    StringBuffer buf = new StringBuffer(1024);
    buf.append(dp.metric)
    .append(' ')
    .append(dp.timestamp)
    .append(' ')
    .append(dp.value);
    buf.append(' ');
    if (dp.tags != null){
      int count = 0;
      for (Map.Entry<String, String> tag : dp.tags.entrySet()){
        if (count>0)
          buf.append(" ");
        buf.append(tag.getKey()).append("=").append(tag.getValue());
        count++;
      }
    }
    return buf.toString();
  }
  
  /**
   * Represents a single data point collected from the system
   */
  public static final class StatsDP {
    public String metric;
    public Number value;
    public long timestamp;
    public Map<String, String> tags;
  }
}

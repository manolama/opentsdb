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
package net.opentsdb.realtime;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.BoundQueue;
import net.opentsdb.utils.Config;

/**
 * Realtime plugins are used to forward data from OpenTSDB to external systems
 * for further analysis. Realtime sinks can be used for backup storage, 
 * aggregation, caching, rollups, etc. Since TSDB is really designed for 
 * storing data points as quickly as possible, secondary functions should be 
 * shunted to other processes or servers so that the TSD can do it's job without
 * running out of RAM or IO. 
 * 
 * When a realtime plugin is enabled, incoming data is sent to the plugin 
 * immediately after it is stored or queued for storage. The implementation 
 * must watch the queues and work with the data as quickly as possible.
 * 
 * @warn This class does NOT GAURANTEE that data will be stored in a queue for
 * publishing. We can't afford to drag down the main TSD storage process so 
 * if the implementation is unable to clear the blocking queue fast enough, 
 * data will be lost (though it should still be in storage). 
 * 
 * @note The actual queuing is handled by TSDB as we don't want implementations
 * to block the main process. 
 * 
 * @since 2.0
 */
public abstract class RealTimePlugin {
  private static final Logger LOG = 
    LoggerFactory.getLogger(RealTimePlugin.class);
  
  /** Queue for timeseries data points */
  protected BoundQueue<DataPoint> datapoint_queue = null;
  
  /** Queue for annotations */
  protected BoundQueue<Annotation> annotation_queue = null;
  
  /** Queue for timeseries metadata */
  protected BoundQueue<TSMeta> tsmeta_queue = null;
  
  /** Queue for UID metadata */
  protected BoundQueue<UIDMeta> uidmeta_queue = null;
  
  /**
   * Called by TSDB to initialize the plugin. Implementations must:
   * - create the queue objects, setting the bounds on the queue using 
   *   properties from the config. The properties are named 
   *   "tsd.realtime.queue_size.<type>" where type is either "datapoints", 
   *   "annotations", "tsmeta" or "uidmeta"
   * - initialize any necessary processes
   * - create a small pool of worker threads and start them
   * - let each thread wait on the queue.take() method of each queue for data
   * 
   * @param config The parent configuration object to pull data from such as
   * queue sizes and client connection information.
   * @return ?
   */
  public abstract Deferred<Object> initialize(final Config config);
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close 
   * any connections or IO they have open
   * @return ?
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Attempts to queue a timeseries data point
   * @param metric Name of the metric
   * @param timestamp Unix epoch timestamp in seconds or milliseconds
   * @param value Numeric value, will either be a long or a float
   * @param tags Hash map of tagk/tagv pairs
   * @param tsuid The UID of the timeseries in bytes
   */
  public final void publishDataPoint (
      final String metric,
      final long timestamp,
      final Object value,
      final Map<String, String> tags, 
      final byte[] tsuid){
    if (this.datapoint_queue != null) {
      this.datapoint_queue.offer(new DataPoint(
          metric, timestamp, value, tags, tsuid));
    }
  }
  
  /**
   * Attempts to queue a timeseries meta object
   * @param meta The timeseries object to queue
   */
  public final void publishTSMeta(final TSMeta meta) {
    if (this.tsmeta_queue != null) {
      this.tsmeta_queue.offer(meta);
    }
  }
  
  /**
   * Attempts to queue a UID meta object
   * @param meta The UID object to queue
   */
  public final void publishUIDMeta(final UIDMeta meta) {
    if (this.uidmeta_queue != null) {
      this.uidmeta_queue.offer(meta);
    }
  }
  
  /**
   * Attempts to queue an annotation
   * @param note The annotation to queue
   */
  public final void publishAnnotation(final Annotation note) {
    if (this.annotation_queue != null) {
      this.annotation_queue.offer(note);
    }
  }
  
  /**
   * Gathers statistics about the running plugin
   * Implementations should call super() first thing, then add their own
   * stats if desired
   * @param collector The collector to store values in
   */
  public void collectStats(final StatsCollector collector) {
    if (this.datapoint_queue != null) {
      collector.record("tsd.realtime.queue.datapoint.failure", 
          this.datapoint_queue.getFailureCount());
      collector.record("tsd.realtime.queue.datapoint.success", 
          this.datapoint_queue.getSuccessCount());
    }
    if (this.tsmeta_queue != null) {
      collector.record("tsd.realtime.queue.tsmeta.failure", 
          this.tsmeta_queue.getFailureCount());
      collector.record("tsd.realtime.queue.tsmeta.success", 
          this.tsmeta_queue.getSuccessCount());
    }
    if (this.uidmeta_queue != null) {
      collector.record("tsd.realtime.queue.tsmeta.failure", 
          this.uidmeta_queue.getFailureCount());
      collector.record("tsd.realtime.queue.tsmeta.success", 
          this.uidmeta_queue.getSuccessCount());
    }
    if (this.annotation_queue != null) {
      collector.record("tsd.realtime.queue.tsmeta.failure", 
          this.annotation_queue.getFailureCount());
      collector.record("tsd.realtime.queue.tsmeta.success", 
          this.annotation_queue.getSuccessCount());
    }
  }
  
  /**
   * Local class used to organize the data point information into a single
   * object for queing
   * @since 2.0
   */
  protected static class DataPoint {
    final String metric;
    final long timestamp;
    final Object value;
    final Map<String, String> tags;
    final byte[] tsuid;

    public DataPoint(final String metric,
        final long timestamp,
        final Object value,
        final Map<String, String> tags, 
        final byte[] tsuid){
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
      this.tsuid = tsuid;
    } 
  }
}

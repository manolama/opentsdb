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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import net.opentsdb.core.TSDB;

import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter for Collectd JSON data from the Write_HTTP plugin.
 * <p>
 * Example:
 * {"values":[4329984,1445888],"dstypes":["counter","counter"],"dsnames":["read"
 * ,"write"],"time":1328130386,"interval":10,"host":"hobbes-64bit","plugin":
 * "disk","plugin_instance":"sdb","type":"disk_octets","type_instance":""}
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
class FormatCollectd {
  private static final Logger LOG = LoggerFactory
      .getLogger(FormatCollectd.class);

  /**
   * This map allows for overriding plugin names that are shortened in Collectd
   * It also allows the user to supply a configuration file for overloading
   * plugin names using the "tsd.formatter.collectd.override" setting in the
   * OpenTSDB.conf file
   */
  @JsonIgnore
  private final Map<String, String> plugin_overrides = new HashMap<String, String>();
  
  // each array should have the same number of elements, if not, there was a
  // glitch
  /** Numeric values of the metric */
  private ArrayList<String> values = new ArrayList<String>();
  /** Type of data the value represents, e.g. counter, gauge, etc */
  private ArrayList<String> dstypes = new ArrayList<String>();
  /** Names of the different values */
  private ArrayList<String> dsnames = new ArrayList<String>();
  /** Unix epoch timestamp */
  private long time = 0;
  /** How often this metric is sent. Can use this for metadata in the future */
  @SuppressWarnings("unused")
  private long interval = 0;
  /** Host from whence the data came */
  private String host = "";
  /** Name of the plugin that generated the metric */
  private String plugin = "";
  /** This is usually the device instance, such as "sdb" or "ef0" */
  private String plugin_instance = "";
  /** The type of data represented in the metric such as "disk_octets" */
  private String type = "";
  /** Alternative instance name */
  private String type_instance = "";

  /**
   * Empty constructor necessary for Jackson deserialization
   */
  public FormatCollectd(){
    
  }
  
  /**
   * Default Constructor to assign the TSD
   * @param tsd TSD to work with
   */
  public FormatCollectd(final TSDB tsdb){
    String path = tsdb.getConfig().formatterCollectdOverride();
    if (!path.isEmpty()) {
      Properties props = new Properties();
      try {
        props.load(new FileInputStream(path));
        if (props.size() > 0) {
          @SuppressWarnings("rawtypes")
          Enumeration e = props.propertyNames();
          while (e.hasMoreElements()) {
            final String key = (String) e.nextElement();
            plugin_overrides.put(key, props.getProperty(key));
          }
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
    } else {
      plugin_overrides.put("if", "interface");
      plugin_overrides.put("df", "disk");
    }
  }

  /**
   * Attempts to parse the posted string for metrics and stores them in the
   * metrics arraylist.
   * @param content The content posted to OpenTSDB
   * @param metrics An array of metrics to store results int
   * @return True if parsing was successful, false if there was an error
   */
  public final boolean parseMetrics(final String content,
      final ArrayList<Metric> metrics, final JSON_HTTP json) {
    final TypeReference<ArrayList<FormatCollectd>> typeRef = 
      new TypeReference<ArrayList<FormatCollectd>>() {
    };

    
    if (json.parseObject(content, typeRef)) {
      @SuppressWarnings("unchecked")
      ArrayList<FormatCollectd> mets = (ArrayList<FormatCollectd>) json
          .getObject();
      if (mets == null || mets.size() < 1)
        return false;
      
      for (FormatCollectd cd : mets) {
        Metric ms[] = cd.getMetrics();
        if (ms == null || ms.length < 1){
          LOG.warn("Unable to extract metrics");
          continue;
        }
        for (Metric m : ms)
          metrics.add(m);
      }
    } else {
      return false;
    }
    return true;
  }

  /**
   * Builds an array of Metric objects to store in HBase
   * @return Null if there was an error, otherwise an array of at least one
   *         metric for storage
   */
  private final Metric[] getMetrics() {
    // data validation checks
    if (values.size() < 1) {
      LOG.error("Missing metric values");
      return null;
    }
    if (time < 1) {
      LOG.error("Invalid time [" + time + "]");
      return null;
    }
    if (plugin.isEmpty()) {
      LOG.error("Missing plugin name");
      return null;
    }
    // in older versions, we don't have dstypes
    if (dstypes.size() > 0 && (values.size() != dstypes.size() || 
        dstypes.size() != dsnames.size())) {
      LOG.error("One of the array sizes is off!");
      // TODO log which one is off
      return null;
    }

    // loop and dump
    Metric[] metrics = new Metric[values.size()];
    for (int i = 0; i < values.size(); i++) {
      Metric m = new Metric();

      // set value and timestamp
      m.setTimestamp(time);
      m.setValue(this.values.get(i));

      // build metric name with some dedupe thrown in
      StringBuilder metric = new StringBuilder(
          plugin_overrides.containsKey(plugin) ? plugin_overrides.get(plugin)
              : plugin);
      if (dsnames.size() > 0 && !dsnames.get(i).equals(plugin)) {
        metric.append(".");
        metric.append(dsnames.get(i));
      }
      if (dsnames.size() > 0 && !type.isEmpty() && !dsnames.get(i).equals(type)
          && !plugin.equals(type)) {
        metric.append(".");
        metric.append(type);
      }
      m.setMetric(metric.toString());

      // add host tag
      m.setTags(new HashMap<String, String>());
      m.getTags().put("host", host);

      // add tags
      if (!plugin_instance.isEmpty())
        m.getTags().put("instance", plugin_instance);
      else if (!type_instance.isEmpty())
        m.getTags().put("instance", type_instance);

      // add the metric
      metrics[i] = m;
    }

    // return
    return metrics;
  }

  /**
   * Method used by Jackson to capture unknown fields and log them. Without this
   * if the JSON format changes slightly (by adding a field) we wouldn't be able
   * to deserialize automatically. This method simply logs a notification.
   * @param key The JSON parameter
   * @param value The value of the JSON parameter
   */
  @SuppressWarnings("unused")
  /* is used by Jackson */
  @JsonAnySetter
  private void handleUnknown(String key, Object value) {
    LOG.warn("Unrecognized JSON parameter [" + key + "] Value ["
        + value.toString() + "]");
  }
}

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
package net.opentsdb.core;

import java.util.Properties;

import net.opentsdb.core.TSDB.TSDRole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all of the settings for a TSD that are user configurable.
 * <p>
 * On initialization, the class will attempt to load it's configuration from multiple
 * locations. The Config class should be initialized before any other TSDB classes.
 * The config file format is a standard java.properties file with
 * key=value pairs and the pound sign signifying comments.
 * <p>
 * When accessing a parameter, the following steps are taken:
 * 1. It looks up the parameter in the properties map. If it's not there, the default
 * value is returned.
 * 2. If the parameter is numeric or boolean, it's parsed. If an exception is thrown, 
 * the error is logged and the default returned.
 * 3. If there are min/max values around a parameter, those are checked, and if the
 * value is out of bounds, an error is logged and the default is returned.
 * 4. After all of the checks, the user supplied value is returned.
 * <p>
 * If you want to add a configuration setting, you have to do the following: 
 * 1. Create a new field with a default value 
 * 2. Add a max and/or min if it's a numeric value 
 * 3. Create a getter in the proper section  
 * 4. Optional: If it's a CLI value, edit ArgP.overloadConfigs() with the CLI version
 */
public class TsdbConfig {
  private static final Logger LOG = LoggerFactory.getLogger(TsdbConfig.class);

  private ConfigLoader config = new ConfigLoader();

  // -------- TSD -------
  /** Whether or not this TSD instance is read-only */
  private boolean read_only = false;
  /** Disables automatic metric creation */
  private boolean auto_metric = false;
  /** Path to write cache files to */
  private String cache_directory = "/tmp/tsd";
  /** Default role for the TSD */
  private TSDRole role = TSDRole.Full;
  private int uid_flush_interval = 5000;

  // -------- Storage -------
  /** The name of the HBase table metric values are stored in */
  private String tsd_table = "tsdb";
  /** The name of the HBase table where metadata is stored */
  private String tsd_uid_table = "tsdb-uid";
  private String storage_handler = "hbase";
  private boolean storage_time_puts = true;
  /** How often, in milliseconds, to flush incoming metrics to HBase */
  private int flush_interval = 1000;
  /** Determines whether or not to enable the compaction process */
  private boolean enable_compactions = false;

  // -------- Network -------
  /** The default network port */
  private int network_port = 4242;
  /** Enables TCP No delay on the socket */
  private boolean network_tcp_nodelay = true;
  /** Enables TCP keepalives on the socket */
  private boolean network_keepalive = true;
  /** Enables reuse of the socket */
  private boolean network_reuse_address = true;

  // -------- HTTP -------
  /**
   * Determines the maximum chunk size to accept for the HTTP chunk aggregator
   * (in bytes)
   */
  private int http_chunk_size = 1024 * 1024;
  /** Amount of RAM to use for the HTTP cache in KB */
  private int http_cache_ram_limit = 32 * 1024;
  /** Largest size data can be to fit in RAM cache in KB */
  private int http_cache_ram_file_limit = 64;
  /** How long, in seconds, to keep suggestion responses in cache */
  private long http_suggest_expire = 60;
  /** Location for static HTTP files */
  private String http_static_root = "staticroot";

  // -------- Search -------
  private String search_index_path = "idx";
  private boolean search_enable_indexer = false;
  private int search_index_interval = 60 * 15;
  private String search_remote_indexers = "";

  // -------- Formatters --------
  /** Path to override map file */
  private String formatter_collectd_override_path = "";
  private String formatter_default_http = "json";
  private String formatter_default_telnet = "ascii";

  // -------- MQ --------
  private boolean mq_enable = false;
  
  
  // --------- LIMITS -------
  private static final int MAX_FLUSH_INTERVAL = 30 * 60 * 1000; // 30 minutes
  private static final int MAX_CHUNK_SIZE = 100 * 1024 * 1024; // big chunk
  private static final int MAX_CACHE_SIZE = 1 * 1024 * 1024; // 1 GB
  private static final long MAX_CACHE_EXPIRE = 60 * 60 * 24; // 1 day

  /**
   * Attempts to load the configuration file from multiple default locations.
   * If no file is found, defaults are used.
   */
  public TsdbConfig() {
    config.loadConfig();
  }
  
  /**
   * Attempts to load the configuration file from the given location, falling back
   * to the default locations. If no config file is found, the defaults will be used.
   * @param config_file Full path to a configuration file
   */
  public TsdbConfig(final String config_file) {
    config.loadConfig(config_file);
  }

  /**
   * Returns all of the configurations stored in the properties list. Note that
   * this is only the properties that have been set via command line or
   * configuration file. Anything not listed will use the defaults
   * @param as_json NOT IMPLEMENTED Returns a JSON string
   * @return A string with all of the settings
   */
  public final String dumpConfiguration(boolean as_json) {
    return config.dumpConfiguration(as_json);
    
    // todo look to see if we can override this using the methods to get
    // user supplied OR default values
    
    // if not asJson, iterate and build a string
//    if (as_json) {
//      // todo may need to move jsonHelper around to access it here
//      // JsonHelper json = new JsonHelper(props);
//      return "";
//    } else {
//      StringBuilder response = new StringBuilder("TSD Configuration:\n");
//      for (Field field : this.getClass().getDeclaredFields()) {
//        response.append(field.getName() + ": ");
//        try {
//          response.append(field.get(this) + "\n");
//        } catch (IllegalArgumentException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        } catch (IllegalAccessException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//          LOG.warn(e.getMessage());
//        }
//      }
//      return response.toString();
//    }
  }

  /**
   * Sets a config in the underying properties map. 
   * NOTE that this should be used with care. The property must be the dotted
   * parameter name.
   * @param property A dotted parameter name, e.g. "tsd.role"
   * @param value The value to store
   */
  public final void setConfig(final String property, final String value){
    this.config.setConfig(property, value);
  }
  
  public final Properties getProperties(){
    return this.config.getProperties();
  }
  
  public final ConfigLoader getConfigLoader(){
    return this.config;
  }
  
  // -------- TSD -------
  /**
   * Whether or not this TSD instance is read-only
   * @return True or false
   */
  public final boolean readOnly() {
    try {
      return this.config.getBoolean("tsd.readonly");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.read_only;
  }

  /**
   * The name of the HBase table metric values are stored in
   * @return Name of the table
   */
  public final String tsdTable() {
    try {
      return this.config.getString("tsd.storage.table.tsd");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.tsd_table;
  }

  /**
   * The name of the HBase table where metadata is stored
   * @return Name of the table
   */
  public final String tsdUIDTable() {
    try {
      return this.config.getString("tsd.storage.table.uid");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.tsd_uid_table;
  }

  /**
   * Whether or not to create new metrics when a new kind of datapoint comes in
   * @return True or false
   */
  public final boolean autoMetric() {
    try {
      return this.config.getBoolean("tsd.autometric");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.auto_metric;
  }

  /**
   * How often, in milliseconds, to flush incoming metrics to HBase
   * @return Flush interval
   */
  public final int flushInterval() {
    try {
      return this.config.getInt("tsd.storage.flushinterval", 0, MAX_FLUSH_INTERVAL);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.flush_interval;
  }

  /**
   * Determines whether or not to enable the compaction process
   * @return
   */
  public final boolean enableCompactions() {
    try {
      return this.config.getBoolean("tsd.storage.hbase.enablecompactions");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.enable_compactions;
  }

  /**
   * Path to write cache files to
   * @return Path
   */
  public final String cacheDirectory() {
    try {
      return this.config.getString("tsd.cache.directory");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.cache_directory;
  }

  public final TSDRole role() {
    try {
      return TSDB.stringToRole(this.config.getString("tsd.role"));
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    } catch (IllegalArgumentException iae) {
      LOG.warn(iae.getLocalizedMessage());
    }
    return this.role;
  }

  public final int uidFlushInterval() {
    try {
      return this.config.getInt("tsd.uid.flushinterval", 1, MAX_FLUSH_INTERVAL);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.uid_flush_interval;
  }
  
  //-------- Storage -------

  public final String storageHandler() {
    try {
      return this.config.getString("tsd.storage.handler");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.storage_handler;
  }
  
  public final boolean storageTimePuts(){
    try {
      return this.config.getBoolean("tsd.storage.timeputs");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.storage_time_puts;
  }
  
  // -------- Network -------
  /**
   * The network port for Telnet and HTTP communications
   * @return The port
   */
  public final int networkPort() {
    try {
      return this.config.getInt("tsd.network.port", 2, 49150);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.network_port;
  }

  /**
   * Enables TCP No delay on the socket
   * @return True or false
   */
  public final boolean networkTCPNoDelay() {
    try {
      return this.config.getBoolean("tsd.network.tcpnodelay");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.network_tcp_nodelay;
  }

  /**
   * Enables TCP keepalives on the socket
   * @return True or false
   */
  public final boolean networkKeepalive() {
    try {
      return this.config.getBoolean("tsd.network.keepalive");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.network_keepalive;
  }

  /**
   * Enables reuse of the socket
   * @return True or false
   */
  public final boolean networkReuseAddress() {
    try {
      return this.config.getBoolean("tsd.network.reuseaddress");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.network_reuse_address;
  }

  // -------- HTTP -------
  /**
   * Determines the maximum chunk size to accept for the HTTP chunk aggregator
   * (in bytes)
   * @return Chunk size
   */
  public final int httpChunkSize() {
    try {
      return this.config.getInt("tsd.http.chunksize", 0, MAX_CHUNK_SIZE);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.http_chunk_size;
  }

  /**
   * Amount of RAM to use for the HTTP cache in KB
   * @return The configured cache size
   */
  public final int httpCacheRamLimit() {
    try {
      return this.config.getInt("tsd.cache.ram.limit", 0, MAX_CACHE_SIZE);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.http_cache_ram_limit;
  }

  /**
   * Largest size data can be to fit in RAM cache in KB
   * @return File size
   */
  public final int httpCacheRamFileLimit() {
    try {
      return this.config
          .getInt("tsd.cache.ram.filelimit", 0, Integer.MAX_VALUE);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.http_cache_ram_file_limit;
  }

  /**
   * How long, in seconds, to keep suggestion responses in cache
   * @return Expiration in seconds
   */
  public final long httpSuggestExpire() {
    try {
      return this.config.getLong("tsd.http.suggest.expire", 0, MAX_CACHE_EXPIRE);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.http_suggest_expire;
  }

  /**
   * Location for static HTTP files
   * @return Path
   */
  public final String httpStaticRoot() {
    try {
      return this.config.getString("tsd.http.staticroot");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.http_static_root;
  }

  // -------- Search --------
  
  public final String searchIndexPath() {
    try {
      return this.config.getString("tsd.search.directory");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.search_index_path;
  }

  public final boolean searchEnableIndexer(){
    try {
      return this.config.getBoolean("tsd.search.enableindexer");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.search_enable_indexer;
  }

  public final int searchIndexInterval(){
    try {
      return this.config.getInt("tsd.search.indexinterval", 60, Integer.MAX_VALUE);
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.search_index_interval;
  }
  
  public final String searchRemoteindexers(){
    try {
      return this.config.getString("tsd.search.remoteindexers");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.search_remote_indexers;
  }
  
  // -------- Formatters -------

  /**
   * Path to override map file
   */
  public final String formatterCollectdOverride() {
    try {
      return this.config.getString("tsd.formatter.collectd.overridespath");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.formatter_collectd_override_path;
  }

  public final String formatterDefaultHTTP() {
    try {
      return this.config.getString("tsd.formatter.default.http");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.formatter_default_http;
  }
  
  public final String formatterDefaultTelnet() {
    try {
      return this.config.getString("tsd.formatter.default.telnet");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.formatter_default_telnet;
  }

  // -------- MQ --------

  public final boolean mqEnable() {
    try {
      return this.config.getBoolean("tsd.mq.enable");
    } catch (NullPointerException npe) {
      // return the default below
    } catch (NumberFormatException nfe) {
      LOG.warn(nfe.getLocalizedMessage());
    }
    return this.mq_enable;
  }
}

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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;

import net.opentsdb.core.TSDB.TSDRole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all of the settings for a TSD that are user configurable.
 * <p>
 * loadConfig() should be called at the start of the application and it will
 * search default locations for config files or it will try to load a user
 * supplied file. The config file format is a standard java.properties file with
 * key=value pairs and the pound sign signifying comments.
 * <p>
 * If you want to add a configuration setting, you have to do the following:
 * 1. Create a new field with a default value
 * 2. Add a max and/or min if it's a numeric value
 * 3. Create a getter and setter in the proper section
 * 4. Add an "else if" in loadProps() to load from a config file
 * 5. Optional: If it's a CLI value, edit ArgP.overloadConfigs() with the
 *    CLI version
 */
public class Config {
  private static final Logger LOG = LoggerFactory.getLogger(Config.class);

  // -------- TSD -------
  /** Whether or not this TSD instance is read-only */
  private boolean read_only = false;
  /** The name of the HBase table metric values are stored in */
  private String tsd_table = "tsdb";
  /** The name of the HBase table where metadata is stored */
  private String tsd_uid_table = "tsdb-uid";
  /** Disables automatic metric creation */
  private boolean auto_metric = false;
  /** Defaults to the localhost for connecting to Zookeeper */
  private String zookeeper_quorum = "localhost";
  /** Path under which is the znode for the -ROOT- region */
  private String zookeeper_base_directory = "/hbase";
  /** How often, in milliseconds, to flush incoming metrics to HBase */
  private int flush_interval = 1000;
  /** Determines whether or not to enable the compaction process */
  private boolean enable_compactions = false;
  /** Path to write cache files to */
  private String cache_directory = "";
  private TSDRole role = TSDRole.API;

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
  private String http_static_root = "";
  
  // -------- FORMATTERS --------
  /** Path to override map file */
  private String formatter_collectd_override_path = "";

  private String search_index_path = "";
  
  // --------- LIMITS -------
  private static final int MAX_FLUSH_INTERVAL = 30 * 60 * 1000; // 30 minutes
  private static final int MAX_CHUNK_SIZE = 100 * 1024 * 1024; // big chunk
  private static final int MAX_CACHE_SIZE = 1 * 1024 * 1024; // 1 GB
  private static final long MAX_CACHE_EXPIRE = 60 * 60 * 24; // 1 day

  /**
   * Attempts to load a configuration file. If the user called Load(String file)
   * with a file, it will try to load only that file. Otherwise it will attempt
   * to load the file from default locations.
   * @param path The full or relative path to a configuration file
   * @return True if a config file was loaded, false if no file was loaded
   */
  public final boolean loadConfig(String path) {

    /** A list of file locations, provided by the user or loads defaults */
    ArrayList<String> file_locations = new ArrayList<String>();

    if (path == null || path.isEmpty()) {
      // search locally first
      file_locations.add("opentsdb.conf");

      // add default locations based on OS
      if (System.getProperty("os.name").contains("Windows")) {
        file_locations.add("C:\\program files\\opentsdb\\opentsdb.conf");
      } else {
        file_locations.add("/etc/opentsdb/opentsdb.conf");
      }
    } else {
      file_locations.add(path);
    }

    // props object to auto-parse and load
    final Properties props = new Properties();
    boolean found_file = false;

    // loop and load until we get something
    for (String file : file_locations) {
      try {
        props.load(new FileInputStream(file));
        LOG.info("Loaded configuration file [" + file + "]");

        // clear the file_locations so we can load again if necessary
        file_locations.clear();
        file_locations = null;
        found_file = true;
        break;
      } catch (FileNotFoundException e) {
        LOG.debug("No configuration file found at: " + file);
      } catch (IOException e) {
        LOG.error("Error loading config file: " + file + " Error: "
            + e.getMessage());
      }
    }

    // dump if we couldn't load
    if (!found_file) {
      // couldn't load anything
      LOG.warn("Unable to load any of the configuration files");
      return false;
    }

    if (props.size() < 1){
      LOG.warn("No properties were found in the configuration file");
      return false;
    }
    
    return this.loadProps(props);
  }

  /**
   * Attempts to load a configuration file specified by the user
   * @return True if a config file was loaded, false if no file was loaded
   */
  public final boolean loadConfig() {
    return loadConfig(null);
  }
  
  /**
   * Returns all of the configurations stored in the properties list. Note that
   * this is only the properties that have been set via command line or
   * configuration file. Anything not listed will use the defaults
   * @param as_json NOT IMPLEMENTED Returns a JSON string
   * @return A string with all of the settings
   */
   public final String dumpConfiguration(boolean as_json) {
    // if not asJson, iterate and build a string
    if (as_json) {
      // todo may need to move jsonHelper around to access it here
      // JsonHelper json = new JsonHelper(props);
      return "";
    } else {
      StringBuilder response = new StringBuilder("TSD Configuration:\n");
      for (Field field : this.getClass().getDeclaredFields()){
        response.append(field.getName() + ": ");
        try {
          response.append(field.get(this) + "\n");
        } catch (IllegalArgumentException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          LOG.warn(e.getMessage());
        }
      }
      return response.toString();
    }
   }
  
  // -------- TSD -------
  /**
   * Whether or not this TSD instance is read-only
   * @return True or false
   */
  public final boolean readOnly() {
    return this.read_only;
  }

  /**
   * Whether or not this TSD instance is read-only
   * @param read_only A flag to tell the TSD to operate in read only mode or not
   * @return A reference to this Config instance
   */
  public final Config readOnly(final boolean read_only) {
    this.read_only = read_only;
    return this;
  }

  /**
   * The name of the HBase table metric values are stored in
   * @return Name of the table
   */
  public final String tsdTable() {
    return this.tsd_table;
  }

  /**
   * The name of the HBase table metric values are stored in
   * @param tsd_table A string with the table name, cannot be empty
   * @return A reference to this Config instance
   */
  public final Config tsdTable(final String tsd_table) {
    if (tsd_table.isEmpty())
      throw new IllegalArgumentException(
          "Missing tsd_table value, must supply a value");
    this.tsd_table = tsd_table;
    return this;
  }

  /**
   * The name of the HBase table where metadata is stored
   * @return Name of the table
   */
  public final String tsdUIDTable() {
    return this.tsd_uid_table;
  }

  /**
   * The name of the HBase table where metadata is stored
   * @param tsd_uid_table A string with the table name, cannot be empty
   * @return A reference to this Config instance
   */
  public final Config tsdUIDTable(final String tsd_uid_table) {
    if (tsd_table.isEmpty())
      throw new IllegalArgumentException(
          "Missing tsd_uid_table value, must supply a value");
    this.tsd_uid_table = tsd_uid_table;
    return this;
  }

  /**
   * Whether or not to create new metrics when a new kind of datapoint comes in
   * @return True or false
   */
  public final boolean autoMetric() {
    return this.auto_metric;
  }

  /**
   * Whether or not to create new metrics when a new kind of datapoint comes in
   * @param auto_metric Flag to set autoMetric
   * @return A reference to this Config instance
   */
  public final Config autoMetric(final boolean auto_metric) {
    this.auto_metric = auto_metric;
    return this;
  }

  /**
   * A comma delimited list of zookeeper hosts to poll for access to the HBase
   * cluster
   * @return The hosts to work with
   */
  public final String zookeeperQuorum() {
    return this.zookeeper_quorum;
  }

  /**
   * A comma delimited list of zookeeper hosts to poll for access to the HBase
   * cluster
   * @param zookeeper_quorum A single hostname or IP or a comma delimited list
   *          of hosts to use. Cannot be empty.
   * @return A reference to this Config instance
   */
  public final Config zookeeperQuorum(final String zookeeper_quorum) {
    if (zookeeper_quorum.isEmpty())
      throw new IllegalArgumentException(
          "Missing zookeeper_quorum value, must supply a value");
    this.zookeeper_quorum = zookeeper_quorum;
    return this;
  }

  /**
   * Path under which is the znode for the -ROOT- region
   * @return Path
   */
  public final String zookeeperBaseDirectory(){
    return this.zookeeper_base_directory;
  }
  
  /**
   * Path under which is the znode for the -ROOT- region
   * @param zookeeper_base_directory Path to use
   * @return A reference to this Config instance
   */
  public final Config zookeeperBaseDirectory(final String zookeeper_base_directory){
    if (zookeeper_base_directory.isEmpty())
      throw new IllegalArgumentException(
          "Missing zookeeper_base_directory value, must supply a value");
    this.zookeeper_base_directory = zookeeper_base_directory;
    return this;
  }
  
  /**
   * How often, in milliseconds, to flush incoming metrics to HBase
   * @return Flush interval
   */
  public final int flushInterval() {
    return this.flush_interval;
  }

  /**
   * How often, in milliseconds, to flush incoming metrics to HBase
   * @param flush_interval A positive number from 0 to MAX_FLUSH_INTERVAL
   * @return A reference to this Config instance
   */
  public final Config flushInterval(final int flush_interval) {
    if (flush_interval < 0)
      throw new IllegalArgumentException(
          "Flush interval must be 0 or greater: " + flush_interval);
    if (flush_interval > MAX_FLUSH_INTERVAL)
      throw new IllegalArgumentException("Flush interval must be less than "
          + MAX_FLUSH_INTERVAL + " ms: " + flush_interval);
    this.flush_interval = flush_interval;
    return this;
  }

  /**
   * Determines whether or not to enable the compaction process
   * @return
   */
  public final boolean enableCompactions(){
    return this.enable_compactions;
  }
  
  /**
   * Determines whether or not to enable the compaction process
   * @param enable_compactions Flag to set
   * @return A reference to this Config instance
   */
  public final Config enableCompactions(final boolean enable_compactions){
    this.enable_compactions = enable_compactions;
    return this;
  }
  
  /**
   * Path to write cache files to
   * @return Path
   */
  public final String cacheDirectory(){
    return this.cache_directory;
  }
  
  /**
   * Path to write cache files to
   * @param cache_directory Path to use
   * @return A reference to this Config instance
   */
  public final Config cacheDirectory(final String cache_directory){
    if (cache_directory.isEmpty())
      throw new IllegalArgumentException("Argument is empty, please supply a directory path");
    this.cache_directory = cache_directory;
    return this;
  }
  
  public final TSDRole role(){
    return this.role;
  }
  
  public final Config role(final String role){
    if (role.isEmpty())
      throw new IllegalArgumentException("Role argument was empty, please supply a role name");
    
    if (role.toUpperCase().compareTo("INGEST") == 0){
      this.role = TSDRole.Ingest;
      return this;
    }
    if (role.toUpperCase().compareTo("FORWARDER") == 0){
      this.role = TSDRole.Forwarder;
      return this;
    }
    if (role.toUpperCase().compareTo("API") == 0){
      this.role = TSDRole.API;
      return this;
    }
    if (role.toUpperCase().compareTo("ROLLER") == 0){
      this.role = TSDRole.Roller;
      return this;
    }
    if (role.toUpperCase().compareTo("ESPER") == 0){
      this.role = TSDRole.Esper;
      return this;
    }
    if (role.toUpperCase().compareTo("TOOL") == 0){
      this.role = TSDRole.Tool;
      return this;
    }
    throw new IllegalArgumentException("Invalid role argument");
  }
  
  // -------- Network -------
  /**
   * The network port for Telnet and HTTP communications
   * @return The port
   */
  public final int networkPort() {
    return this.network_port;
  }

  /**
   * The network port for Telnet and HTTP communications
   * @param network_port A value between 2 and 49150. Above 49150 is reserved
   *          for dynamic processes (think dns servers). Be careful not to steal
   *          some other processes' port
   * @return A reference to this Config instance
   */
  public final Config networkPort(final int network_port) {
    if (network_port > 49150)
      throw new IllegalArgumentException("Network port must be below 49150: "
          + network_port);
    if (network_port < 2)
      throw new IllegalArgumentException("Network port must greater than 1: "
          + network_port);
    this.network_port = network_port;
    return this;
  }

  /**
   * Enables TCP No delay on the socket
   * @return True or false
   */
  public final boolean networkTCPNoDelay() {
    return this.network_tcp_nodelay;
  }

  /**
   * Enables TCP No delay on the socket
   * @param network_tcp_nodelay flag to use
   * @return A reference to this Config instance
   */
  public final Config networkTCPNoDelay(final boolean network_tcp_nodelay) {
    this.network_tcp_nodelay = network_tcp_nodelay;
    return this;
  }

  /**
   * Enables TCP keepalives on the socket
   * @return True or false
   */
  public final boolean networkKeepalive() {
    return this.network_keepalive;
  }

  /**
   * Enables TCP keepalives on the socket
   * @param network_keepalive Flag to set
   * @return A reference to this Config instance
   */
  public final Config networkKeepalive(final boolean network_keepalive) {
    this.network_keepalive = network_keepalive;
    return this;
  }

  /**
   * Enables reuse of the socket
   * @return True or false
   */
  public final boolean networkReuseAddress() {
    return this.network_reuse_address;
  }

  /**
   * Enables reuse of the socket
   * @param network_reuse_address Flag to set
   * @return A reference to this Config instance
   */
  public final Config networkReuseAddress(final boolean network_reuse_address) {
    this.network_reuse_address = network_reuse_address;
    return this;
  }

  // -------- HTTP -------
  /**
   * Determines the maximum chunk size to accept for the HTTP chunk aggregator
   * (in bytes)
   * @return Chunk size
   */
  public final int httpChunkSize() {
    return this.http_chunk_size;
  }

  /**
   * Determines the maximum chunk size to accept for the HTTP chunk aggregator
   * (in bytes)
   * @param http_chunk_size Positive integer from 0 to MAX_CHUNK_SIZE. 0
   *          disables chunking
   * @return A reference to this Config instance
   */
  public final Config httpChunkSize(final int http_chunk_size) {
    if (http_chunk_size < 0)
      throw new IllegalArgumentException("Chunk size must be 0 or greater: "
          + http_chunk_size);
    if (http_chunk_size > MAX_CHUNK_SIZE)
      throw new IllegalArgumentException("Chunk size must be less than: "
          + MAX_CHUNK_SIZE);
    this.http_chunk_size = http_chunk_size;
    return this;
  }

  /**
   * Amount of RAM to use for the HTTP cache in KB
   * @return The configured cache size
   */
  public final int httpCacheRamLimit() {
    return this.http_cache_ram_limit;
  }

  /**
   * Amount of RAM to use for the HTTP cache in KB
   * @param http_cache_ram_limit Cache size from 0 to MAX_CACHE_SIZE. 0 disabled
   *          RAM caching but allows disk caching
   * @return A reference to this Config instance
   */
  public final Config httpCacheRamLimit(final int http_cache_ram_limit) {
    if (http_cache_ram_limit < 0)
      throw new IllegalArgumentException("Cache size must be 0 or greater: "
          + http_cache_ram_limit);
    if (http_cache_ram_limit > MAX_CACHE_SIZE)
      throw new IllegalArgumentException("Cache size must be less than "
          + MAX_CACHE_SIZE + ": " + http_cache_ram_limit);
    this.http_cache_ram_limit = http_cache_ram_limit;
    return this;
  }

  /**
   * Largest size data can be to fit in RAM cache in KB
   * @return File size
   */
  public final int httpCacheRamFileLimit() {
    return this.http_cache_ram_file_limit;
  }

  /**
   * Largest size data can be to fit in RAM cache in KB
   * @param http_cache_ram_file_limit Value from 0 to MAX_CACHE_SIZE
   * @return A reference to this Config instance
   */
  public final Config httpCacheRamFileLimit(final int http_cache_ram_file_limit) {
    if (http_cache_ram_file_limit < 0)
      throw new IllegalArgumentException("Cache size must be 0 or greater: "
          + http_cache_ram_file_limit);
    if (http_cache_ram_file_limit > MAX_CACHE_SIZE)
      throw new IllegalArgumentException("Cache size must be less than "
          + MAX_CACHE_SIZE + ": " + http_cache_ram_limit);
    this.http_cache_ram_file_limit = http_cache_ram_file_limit;
    return this;
  }

  /**
   * How long, in seconds, to keep suggestion responses in cache
   * @return Expiration in seconds
   */
  public final long httpSuggestExpire() {
    return this.http_suggest_expire;
  }

  /**
   * How long, in seconds, to keep suggestion responses in cache
   * @param http_suggest_expire Value from 0 to MAX_CACHE_EXPIRE
   * @return A reference to this Config instance
   */
  public final Config httpSuggestExpire(final long http_suggest_expire) {
    if (http_suggest_expire < 0)
      throw new IllegalArgumentException(
          "Suggestion expiration must be 0 or greater: " + http_suggest_expire);
    if (http_suggest_expire > MAX_CACHE_EXPIRE)
      throw new IllegalArgumentException(
          "Suggestion expiration must be less than " + MAX_CACHE_EXPIRE + ": "
              + http_suggest_expire);
    this.http_suggest_expire = http_suggest_expire;
    return this;
  }

  /**
   * Location for static HTTP files
   * @return Path
   */
  public final String httpStaticRoot(){
    return this.http_static_root;
  }
  
  /**
   * Location for static HTTP files
   * @param static_root Path to use
   * @return A reference to this Config instance
   */
  public final Config httpStaticRoot(final String static_root){
    if (static_root.isEmpty())
      throw new IllegalArgumentException("Path was empty, please supply a valid directory");
    this.http_static_root = static_root;
    return this;
  }
  
  public final String searchIndexPath(){
    return this.search_index_path;
  }
  
  public final Config searchIndexPath(final String path){
    if (path.isEmpty())
      throw new IllegalArgumentException("Path was empty, please supply a valid directory");
    this.search_index_path = path;
    return this;
  }
  
//-------- FORMATTERS --------
  
  /**
   * Path to override map file
   */
  public final String formatterCollectdOverride(){
    return this.formatter_collectd_override_path;
  }
  
  /**
   * Path to override map file
   * @param formatter_collectd_override_path Non-empty path to file
   * @return A reference to this Config instance
   */
  public final Config formatterCollectdOverride(final String formatter_collectd_override_path){
    if (formatter_collectd_override_path.isEmpty())
      throw new IllegalArgumentException("Override path cannot be empty");
    this.formatter_collectd_override_path = formatter_collectd_override_path;
    return this;
  }
  
  /**
   * Loops through a properties file and loads the local configuration
   * instance with values from the object. 
   * @param props A properties object
   * @return True if values were loaded, false if not
   */
  private boolean loadProps(final Properties props){
    if (props.size() < 1)
      throw new IllegalArgumentException("Properties collection is empty");
    
    for (@SuppressWarnings("rawtypes")
    Enumeration e = props.keys(); e.hasMoreElements();) {
      String key = (String) e.nextElement();
      String value = props.getProperty(key);
      
      // now we create a huge if/else block to determine where to store props
      if (key.toLowerCase().equals("read_only")){
        this.readOnly(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("tsd_table")){
        this.tsdTable(value);
      }else if (key.toLowerCase().equals("tsd_uid_table")){
        this.tsdUIDTable(value);
      }else if (key.toLowerCase().equals("auto_metric")){
        this.autoMetric(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("zookeeper_quorum")){
        this.zookeeperQuorum(value);
      }else if (key.toLowerCase().equals("enable_compactions")){
        this.enableCompactions(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("cache_directory")){
        this.cacheDirectory(value);
      }else if (key.toLowerCase().equals("network_port")){
        this.networkPort(Integer.parseInt(value));
      }else if (key.toLowerCase().equals("network_tcp_nodelay")){
        this.networkTCPNoDelay(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("network_keepalive")){
        this.networkKeepalive(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("network_reuse_address")){
        this.networkReuseAddress(Boolean.parseBoolean(value));
      }else if (key.toLowerCase().equals("http_chunk_size")){
        this.httpChunkSize(Integer.parseInt(value));
      }else if (key.toLowerCase().equals("http_cache_ram_limit")){
        this.httpCacheRamLimit(Integer.parseInt(value));
      }else if (key.toLowerCase().equals("http_cache_ram_file_limit")){
        this.httpCacheRamFileLimit(Integer.parseInt(value));
      }else if (key.toLowerCase().equals("http_suggest_expire")){
        this.httpSuggestExpire(Integer.parseInt(value));
      }else if (key.toLowerCase().equals("http_static_root")){
        this.httpStaticRoot(value);
      }else if (key.toLowerCase().equals("formatter_collectd_override")){
        this.formatterCollectdOverride(value);
      }else if (key.toLowerCase().equals("search_index_path")){
        this.searchIndexPath(value);
      }else if (key.toLowerCase().equals("role")){
        this.role(value);
      }else{
        // unrecognized option
        LOG.warn("Unrecognized configuration key: " + key + " = " + value);
      }
    }
    return true;
  }
}

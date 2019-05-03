// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.base.Strings;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.storage.WritableTimeSeriesDataStore;

/**
 * A base class for data stores that implement the TSDB v1 schema of
 * encoding data in wide column store like HBase and Bigtable.
 * 
 * TODO - more work on this
 * 
 * @since 3.0
 */
public abstract class Tsdb1xDataStore implements WritableTimeSeriesDataStore {
  
  /** Config keys */
  public static final String CONFIG_PREFIX = "tsd.storage.";
  public static final String DATA_TABLE_KEY = "data_table";
  public static final String UID_TABLE_KEY = "uid_table";
  public static final String TREE_TABLE_KEY = "tree_table";
  public static final String META_TABLE_KEY = "meta_table";
  
  // TODO - convert these to named settings.
  public static final String MULTI_GET_CONCURRENT_KEY = "tsd.query.multiget.concurrent";
  public static final String MULTI_GET_BATCH_KEY = "tsd.query.multiget.batch_size";
  public static final String EXPANSION_LIMIT_KEY = 
      "tsd.query.filter.expansion_limit";
  public static final String ROLLUP_USAGE_KEY = 
      "tsd.query.rollups.default_usage";
  public static final String SKIP_NSUN_TAGK_KEY = "tsd.query.skip_unresolved_tagks";
  public static final String SKIP_NSUN_TAGV_KEY = "tsd.query.skip_unresolved_tagvs";
  public static final String SKIP_NSUI_KEY = "tsd.query.skip_unresolved_ids";
  public static final String ALLOW_DELETE_KEY = "tsd.query.allow_delete";
  public static final String DELETE_KEY = "tsd.query.delete";
  public static final String PRE_AGG_KEY = "tsd.query.pre_agg";
  public static final String FUZZY_FILTER_KEY = "tsd.query.enable_fuzzy_filter";
  public static final String ROWS_PER_SCAN_KEY = "tsd.query.rows_per_scan";
  public static final String MAX_MG_CARDINALITY_KEY = "tsd.query.multiget.max_cardinality";
  public static final String ENABLE_APPENDS_KEY = "tsd.storage.enable_appends";
  public static final String ENABLE_COPROC_APPENDS_KEY = "tsd.storage.enable_appends_coproc";
  
  public static final byte[] DATA_FAMILY = 
      "t".getBytes(Const.ISO_8859_CHARSET);
  
  protected final Tsdb1xDataStoreFactory factory;
  protected final String id;
  protected final Schema schema;
  
  /** Name of the table in which timeseries are stored.  */
  protected final byte[] data_table;
  
  /** Name of the table in which UID information is stored. */
  protected final byte[] uid_table;
  
  /** Name of the table where tree data is stored. */
  protected final byte[] tree_table;
  
  /** Name of the table where meta data is stored. */
  protected final byte[] meta_table;
  
  public Tsdb1xDataStore(final Tsdb1xDataStoreFactory factory, 
                         final String id, 
                         final Schema schema) {
    this.factory = factory;
    this.id = id;
    this.schema = schema;

    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = factory.tsdb().getConfig();
    synchronized(config) {
      if (!config.hasProperty(getConfigKey(DATA_TABLE_KEY))) {
        config.register(getConfigKey(DATA_TABLE_KEY), "tsdb", false, 
            "The name of the raw data table for OpenTSDB.");
      }
      data_table = config.getString(getConfigKey(DATA_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(UID_TABLE_KEY))) {
        config.register(getConfigKey(UID_TABLE_KEY), "tsdb-uid", false, 
            "The name of the UID mapping table for OpenTSDB.");
      }
      uid_table = config.getString(getConfigKey(UID_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(TREE_TABLE_KEY))) {
        config.register(getConfigKey(TREE_TABLE_KEY), "tsdb-tree", false, 
            "The name of the Tree table for OpenTSDB.");
      }
      tree_table = config.getString(getConfigKey(TREE_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      if (!config.hasProperty(getConfigKey(META_TABLE_KEY))) {
        config.register(getConfigKey(META_TABLE_KEY), "tsdb-meta", false, 
            "The name of the Meta data table for OpenTSDB.");
      }
      meta_table = config.getString(getConfigKey(META_TABLE_KEY))
          .getBytes(Const.ISO_8859_CHARSET);
      
      // more bits
      if (!config.hasProperty(EXPANSION_LIMIT_KEY)) {
        config.register(EXPANSION_LIMIT_KEY, 4096, true,
            "The maximum number of UIDs to expand in a literal filter "
            + "for HBase scanners.");
      }
      if (!config.hasProperty(ROLLUP_USAGE_KEY)) {
        config.register(ROLLUP_USAGE_KEY, "rollup_fallback", true,
            "The default fallback operation for queries involving rollup tables.");
      }
      if (!config.hasProperty(SKIP_NSUN_TAGK_KEY)) {
        config.register(SKIP_NSUN_TAGK_KEY, "false", true,
            "Whether or not to simply drop tag keys (names) from query filters "
            + "that have not been assigned UIDs and try to fetch data anyway.");
      }
      if (!config.hasProperty(SKIP_NSUN_TAGV_KEY)) {
        config.register(SKIP_NSUN_TAGV_KEY, "false", true,
            "Whether or not to simply drop tag values from query filters "
            + "that have not been assigned UIDs and try to fetch data anyway.");
      }
      if (!config.hasProperty(SKIP_NSUI_KEY)) {
        config.register(SKIP_NSUI_KEY, "false", true,
            "Whether or not to ignore data from storage that did not "
            + "resolve from a UID to a string. If not ignored, "
            + "exceptions are thrown when the data is read.");
      }
      if (!config.hasProperty(ALLOW_DELETE_KEY)) {
        config.register(ALLOW_DELETE_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(DELETE_KEY)) {
        config.register(DELETE_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(PRE_AGG_KEY)) {
        config.register(PRE_AGG_KEY, "false", true,
            "TODO");
      }
      if (!config.hasProperty(FUZZY_FILTER_KEY)) {
        config.register(FUZZY_FILTER_KEY, "true", true,
            "TODO");
      }
      if (!config.hasProperty(ROWS_PER_SCAN_KEY)) {
        config.register(ROWS_PER_SCAN_KEY, "128", true,
            "TODO");
      }
      if (!config.hasProperty(MULTI_GET_CONCURRENT_KEY)) {
        config.register(MULTI_GET_CONCURRENT_KEY, "20", true,
            "TODO");
      }
      if (!config.hasProperty(MULTI_GET_BATCH_KEY)) {
        config.register(MULTI_GET_BATCH_KEY, "1024", true,
            "TODO");
      }
      if (!config.hasProperty(MAX_MG_CARDINALITY_KEY)) {
        config.register(MAX_MG_CARDINALITY_KEY, "128", true,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_APPENDS_KEY)) {
        config.register(ENABLE_APPENDS_KEY, false, false,
            "TODO");
      }
      if (!config.hasProperty(ENABLE_COPROC_APPENDS_KEY)) {
        config.register(ENABLE_COPROC_APPENDS_KEY, false, false,
            "TODO");
      }
    }
  }
  
  /**
   * Instantiates a new node using the given context and config.
   * @param context A non-null query pipeline context.
   * @param config A query node config. May be null if the node does not
   * require a configuration.
   * @return An instantiated node if successful.
   */
  public abstract QueryNode newNode(final QueryPipelineContext context, 
                                    final QueryNodeConfig config);
  
  /** @return The ID of this time series data store instance. */
  public String id() {
    return id;
  }
  
  public Schema schema() {
    return schema;
  }
  
  /**
   * 
   * @param suffix
   * @return
   */
  public String getConfigKey(final String suffix) {
    return CONFIG_PREFIX + (Strings.isNullOrEmpty(id) ? "" : id + ".")
      + suffix;
  }
  
  /** @return The data table. */
  public byte[] dataTable() {
    return data_table;
  }
  
  /** @return The UID table. */
  public byte[] uidTable() {
    return uid_table;
  }
  
  /** @return The TSDB reference. */
  public TSDB tsdb() {
    return factory.tsdb();
  }
}

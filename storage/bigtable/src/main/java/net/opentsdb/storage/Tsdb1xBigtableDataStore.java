// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.utils.Pair;

public class Tsdb1xBigtableDataStore implements Tsdb1xDataStore {
  
  /** Config keys */
  public static final String CONFIG_PREFIX = "tsd.storage.";
  public static final String DATA_TABLE_KEY = "data_table";
  public static final String UID_TABLE_KEY = "uid_table";
  public static final String TREE_TABLE_KEY = "tree_table";
  public static final String META_TABLE_KEY = "meta_table";
  
  /** Bigtable config keys. */
  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  public static final String INSTANCE_ID_KEY = "google.bigtable.instance.id";
  public static final String ZONE_ID_KEY = "google.bigtable.zone.id";
  public static final String SERVICE_ACCOUNT_ENABLE_KEY = 
      "google.bigtable.auth.service.account.enable";
  public static final String JSON_KEYFILE_KEY = "google.bigtable.auth.json.keyfile";
  public static final String CHANNEL_COUNT_KEY = "google.bigtable.grpc.channel.count";
  
  // TODO  - move to common location
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

  public static final byte[] DATA_FAMILY = 
      "t".getBytes(Const.ISO_8859_CHARSET);
  
  protected final TSDB tsdb;
  protected final String id;
  
  protected final BigtableInstanceName table_namer;
  protected final BigtableSession session;
  protected AsyncExecutor executor;
  protected ExecutorService pool;
  
  protected Schema schema;
  
  private final boolean enable_appends;
  
  private final BulkMutation mutation_buffer;
  
  protected final Tsdb1xBigtableUniqueIdStore uid_store;
  
  /** Name of the table in which timeseries are stored.  */
  protected final byte[] data_table;
  
  /** Name of the table in which UID information is stored. */
  protected final byte[] uid_table;
  
  /** Name of the table where tree data is stored. */
  protected final byte[] tree_table;
  
  /** Name of the table where meta data is stored. */
  protected final byte[] meta_table;
  
  Tsdb1xBigtableDataStore(final Tsdb1xBigtableFactory factory,
                          final String id,
                          final Schema schema) {
    this.tsdb = factory.tsdb();
    this.id = id;
    this.schema = schema;
    pool = Executors.newCachedThreadPool();
    
    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = tsdb.getConfig();
    synchronized(config) {
      if (!config.hasProperty(getConfigKey(DATA_TABLE_KEY))) {
        System.out.println("REGISTERED: " + getConfigKey(DATA_TABLE_KEY));
        config.register(getConfigKey(DATA_TABLE_KEY), "tsdb", false, 
            "The name of the raw data table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(UID_TABLE_KEY))) {
        config.register(getConfigKey(UID_TABLE_KEY), "tsdb-uid", false, 
            "The name of the UID mapping table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(TREE_TABLE_KEY))) {
        config.register(getConfigKey(TREE_TABLE_KEY), "tsdb-tree", false, 
            "The name of the Tree table for OpenTSDB.");
      }
      
      if (!config.hasProperty(getConfigKey(META_TABLE_KEY))) {
        config.register(getConfigKey(META_TABLE_KEY), "tsdb-meta", false, 
            "The name of the Meta data table for OpenTSDB.");
      }
      if (!config.hasProperty(ENABLE_APPENDS_KEY)) {
        config.register(ENABLE_APPENDS_KEY, false, false,
            "TODO");
      }
      

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
      
      // bigtable configs
      if (!config.hasProperty(PROJECT_ID_KEY)) {
        config.register(PROJECT_ID_KEY, null, false, 
            "The project ID hosting the Bigtable cluster");
      }
      if (!config.hasProperty(INSTANCE_ID_KEY)) {
        config.register(INSTANCE_ID_KEY, null, false, 
            "The cluster ID assigned to the Bigtable cluster at creation.");
      }
      if (!config.hasProperty(ZONE_ID_KEY)) {
        config.register(ZONE_ID_KEY, null, false, 
            "The name of the zone where the Bigtable cluster is operating.");
      }
      if (!config.hasProperty(SERVICE_ACCOUNT_ENABLE_KEY)) {
        config.register(SERVICE_ACCOUNT_ENABLE_KEY, true, false, 
            "Whether or not to use a Google cloud service account to connect.");
      }
      if (!config.hasProperty(JSON_KEYFILE_KEY)) {
        config.register(JSON_KEYFILE_KEY, null, false, 
            "The full path to the JSON formatted key file associated with "
            + "the service account you want to use for Bigtable access. "
            + "Download this from your cloud console.");
      }
      if (!config.hasProperty(CHANNEL_COUNT_KEY)) {
        config.register(CHANNEL_COUNT_KEY, 
            Runtime.getRuntime().availableProcessors(), false, 
            "The number of sockets opened to the Bigtable API for handling "
            + "RPCs. For higher throughput consider increasing the "
            + "channel count.");
      }
    }
    
    table_namer = new BigtableInstanceName(
        config.getString(PROJECT_ID_KEY), config.getString(INSTANCE_ID_KEY));
    
    data_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(DATA_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    uid_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(UID_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    tree_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(TREE_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    meta_table = (table_namer.toTableNameStr(
        config.getString(getConfigKey(META_TABLE_KEY))))
          .getBytes(Const.ISO_8859_CHARSET);
    enable_appends = config.getBoolean(ENABLE_APPENDS_KEY);
    
    try {
      final CredentialOptions creds = CredentialOptions.jsonCredentials(
          new FileInputStream(config.getString(JSON_KEYFILE_KEY)));
      
      session = new BigtableSession(new BigtableOptions.Builder()
          .setProjectId(config.getString(PROJECT_ID_KEY))
          .setInstanceId(config.getString(INSTANCE_ID_KEY))
          .setCredentialOptions(creds)
          .setUserAgent("OpenTSDB_3x")
          .setAdminHost(BigtableOptions.BIGTABLE_ADMIN_HOST_DEFAULT)
          .setAppProfileId(BigtableOptions.BIGTABLE_APP_PROFILE_DEFAULT)
          .setPort(BigtableOptions.BIGTABLE_PORT_DEFAULT)
          .setDataHost(BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT)
          .build());
      
      final BigtableTableName data_table_name = new BigtableTableName(
          table_namer.toTableNameStr(config.getString(getConfigKey(DATA_TABLE_KEY))));
      mutation_buffer = session.createBulkMutation(data_table_name);
    } catch (IOException e) {
      throw new StorageException("WTF?", e);
    }
    
    executor = session.createAsyncExecutor();
    
    uid_store = new Tsdb1xBigtableUniqueIdStore(this);
    tsdb.getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ? 
        "default_uidstore" : id + "_uidstore", uid_store);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context, 
                           final String id,
                           final QueryNodeConfig config) {
    return new Tsdb1xBigtableQueryNode(this, context, id, (QuerySourceConfig) config);
  }

  @Override
  public String id() {
    return "Bigtable";
  }

  public Deferred<Object> shutdown() {
    try {
      session.close();
    } catch (IOException e) {
      Deferred.fromError(e);
    }
    return Deferred.fromResult(null);
  }

  /**
   * Prepends the {@link #CONFIG_PREFIX} and the current data store ID to
   * the given suffix.
   * @param suffix A non-null and non-empty suffix.
   * @return A non-null and non-empty config string.
   */
  public String getConfigKey(final String suffix) {
    if (Strings.isNullOrEmpty(suffix)) {
      throw new IllegalArgumentException("Suffix cannot be null.");
    }
    if (Strings.isNullOrEmpty(id)) {
      return CONFIG_PREFIX + suffix;
    } else {
      return CONFIG_PREFIX + id + "." + suffix;
    }
  }
  
  /** @return The schema assigned to this store. */
  Schema schema() {
    return schema;
  }
  
  /** @return The data table. */
  byte[] dataTable() {
    return data_table;
  }
  
  /** @return The UID table. */
  byte[] uidTable() {
    return uid_table;
  }
  
  ExecutorService pool() {
    return pool;
  }
  
  /** @return The Bigtable executor. */
  AsyncExecutor executor() {
    return executor;
  }

  /** @return The TSDB reference. */
  TSDB tsdb() {
    return tsdb;
  }
  
  /** @return The UID store. */
  UniqueIdStore uidStore() {
    return uid_store;
  }

  String dynamicString(final String key) {
    return tsdb.getConfig().getString(key);
  }
  
  int dynamicInt(final String key) {
    return tsdb.getConfig().getInt(key);
  }
  
  boolean dynamicBoolean(final String key) {
    return tsdb.getConfig().getBoolean(key);
  }

  @Override
  public Deferred<WriteStatus> write(AuthState state, TimeSeriesDatum datum,
      Span span) {
 // TODO - other types
    if (datum.value().type() != NumericType.TYPE) {
      return Deferred.fromResult(WriteStatus.rejected(
          "Not handling this type yet: " + datum.value().type()));
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".write")
          .start();
    } else {
      child = null;
    }
    // no need to validate here, schema does it.
    
//    class SuccessCB implements Callback<WriteStatus, Object> {
//      @Override
//      public WriteStatus call(final Object ignored) throws Exception {
//        if (child != null) {
//          child.setSuccessTags().finish();
//        }
//        return WriteStatus.OK;
//      }
//    }
//    
//    class WriteErrorCB implements Callback<WriteStatus, Exception> {
//      @Override
//      public WriteStatus call(final Exception ex) throws Exception {
//        // TODO log?
//        // TODO - how do we retry?
////        if (ex instanceof PleaseThrottleException ||
////            ex instanceof RecoverableException) {
////          if (child != null) {
////            child.setErrorTags(ex)
////                 .finish();
////          }
////          return WriteStatus.retry("Please retry at a later time.");
////        }
//        if (child != null) {
//          child.setErrorTags(ex)
//               .finish();
//        }
//        return WriteStatus.error(ex.getMessage(), ex);
//      }
//    }
    
    class RowKeyCB implements Callback<Deferred<WriteStatus>, IdOrError> {

      @Override
      public Deferred<WriteStatus> call(final IdOrError ioe) throws Exception {
        if (ioe.id() == null) {
          if (child != null) {
            child.setErrorTags(ioe.exception())
                 .setTag("state", ioe.state().toString())
                 .setTag("message", ioe.error())
                 .finish();
          }
          switch (ioe.state()) {
          case RETRY:
            return Deferred.fromResult(WriteStatus.retry(ioe.error()));
          case REJECTED:
            return Deferred.fromResult(WriteStatus.rejected(ioe.error()));
          case ERROR:
            return Deferred.fromResult(WriteStatus.error(ioe.error(), ioe.exception()));
          default:
            throw new StorageException("Unexpected resolution state: " 
                + ioe.state());
          }
        }
        // TODO - handle different types
        long base_time = datum.value().timestamp().epoch();
        base_time = base_time - (base_time % Schema.MAX_RAW_TIMESPAN);
        Pair<byte[], byte[]> pair = schema.encode(datum.value(), 
            enable_appends, (int) base_time, null);
        
        if (enable_appends) {
          final ReadModifyWriteRowRequest append_request = 
              ReadModifyWriteRowRequest.newBuilder()
              .setTableNameBytes(ByteStringer.wrap(data_table))
              .setRowKey(ByteStringer.wrap(ioe.id()))
              .addRules(ReadModifyWriteRule.newBuilder()
                  .setFamilyNameBytes(ByteStringer.wrap(DATA_FAMILY))
                  .setColumnQualifier(ByteStringer.wrap(pair.getKey()))
                  .setAppendValue(ByteStringer.wrap(pair.getValue())))
              .build();
          
          final Deferred<WriteStatus> deferred = new Deferred<WriteStatus>();
          class AppendCB implements FutureCallback<ReadModifyWriteRowResponse> {

            @Override
            public void onSuccess(
                final ReadModifyWriteRowResponse result) {
              if (child != null) {
                child.setSuccessTags().finish();
              }
              
              deferred.callback(WriteStatus.OK);
            }

            @Override
            public void onFailure(final Throwable t) {
              // TODO log?
              // TODO - how do we retry?
//              if (ex instanceof PleaseThrottleException ||
//                  ex instanceof RecoverableException) {
//                if (child != null) {
//                  child.setErrorTags(ex)
//                       .finish();
//                }
//                return WriteStatus.retry("Please retry at a later time.");
//              }
              if (child != null) {
                child.setErrorTags(t)
                     .finish();
              }
              deferred.callback(WriteStatus.error(t.getMessage(), t));
            }
            
          }
          
          Futures.addCallback(
              executor.readModifyWriteRowAsync(append_request), 
              new AppendCB(), 
              pool);
          return deferred;
//          return client.append(new AppendRequest(data_table,ioe.id(), 
//              DATA_FAMILY, pair.getKey(), pair.getValue()))
//              .addCallbacks(new SuccessCB(), new WriteErrorCB());
        } else {
          final MutateRowRequest mutate_row_request = 
              MutateRowRequest.newBuilder()
              .setTableNameBytes(ByteStringer.wrap(data_table))
              .setRowKey(ByteStringer.wrap(ioe.id()))
              .addMutations(Mutation.newBuilder()
                  .setSetCell(SetCell.newBuilder()
                      .setFamilyNameBytes(ByteStringer.wrap(DATA_FAMILY))
                      .setColumnQualifier(ByteStringer.wrap(pair.getKey()))
                      .setValue(ByteStringer.wrap(pair.getValue()))))
              .build();
          
          final Deferred<WriteStatus> deferred = new Deferred<WriteStatus>();
          class PutCB implements FutureCallback<MutateRowResponse> {

            @Override
            public void onSuccess(final MutateRowResponse result) {
              if (child != null) {
                child.setSuccessTags().finish();
              }
              
              deferred.callback(WriteStatus.OK);
            }

            @Override
            public void onFailure(Throwable t) {
           // TODO log?
              // TODO - how do we retry?
//              if (ex instanceof PleaseThrottleException ||
//                  ex instanceof RecoverableException) {
//                if (child != null) {
//                  child.setErrorTags(ex)
//                       .finish();
//                }
//                return WriteStatus.retry("Please retry at a later time.");
//              }
              if (child != null) {
                child.setErrorTags(t)
                     .finish();
              }
              deferred.callback(WriteStatus.error(t.getMessage(), t));
            }
            
          }
          
          Futures.addCallback(
              mutation_buffer.add(mutate_row_request),
              new PutCB(), 
              pool);
          return deferred;
//          return client.put(new PutRequest(data_table, ioe.id(), 
//              DATA_FAMILY, pair.getKey(), pair.getValue()))
//              .addCallbacks(new SuccessCB(), new WriteErrorCB());
        }
      }
      
    }
    
    class ErrorCB implements Callback<WriteStatus, Exception> {
      @Override
      public WriteStatus call(final Exception ex) throws Exception {
        return WriteStatus.error(ex.getMessage(), ex);
      }
    }
    
    try {
      return schema.createRowKey(state, datum, null, child)
          .addCallbackDeferring(new RowKeyCB())
          .addErrback(new ErrorCB());
    } catch (Exception e) {
      // TODO - log
      return Deferred.fromResult(WriteStatus.error(e.getMessage(), e));
    }
  }

  @Override
  public Deferred<List<WriteStatus>> write(AuthState state,
      TimeSeriesSharedTagsAndTimeData data, Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * <p>wasMutationApplied.</p>
   *<b>NOTE</b> Cribbed from Bigtable client.
   * @param request a {@link com.google.bigtable.v2.CheckAndMutateRowRequest} object.
   * @param response a {@link com.google.bigtable.v2.CheckAndMutateRowResponse} object.
   * @return a boolean.
   */
  public static boolean wasMutationApplied(
      CheckAndMutateRowRequest request,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (request.getTrueMutationsCount() > 0
        && response.getPredicateMatched())
        || (request.getFalseMutationsCount() > 0
        && !response.getPredicateMatched());
  }
}

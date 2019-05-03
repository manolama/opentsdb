// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.util.List;

import org.hbase.async.AppendRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.hbase.async.RecoverableException;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesSharedTagsAndTimeData;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.utils.Pair;

/**
 * TODO - complete.
 * 
 * @since 3.0
 */
public class Tsdb1xHBaseDataStore extends Tsdb1xDataStore {
  
  /** AsyncHBase config. */
  public static final String ZNODE_PARENT_KEY = "zookeeper.znode.parent";
  public static final String ZK_QUORUM_KEY = "zookeeper.quorum";
  public static final String AUTH_ENABLED_KEY = "auth.enable";
  public static final String KB_PRINCIPAL_KEY = "kerberos.principal";
  public static final String KB_ENABLED_KEY = "kerberos.enable";
  public static final String SASL_CLIENT_KEY = "sasl.clientconfig";
  public static final String META_SPLIT_KEY = "meta.split";
  
  /** The AsyncHBase client. */
  private HBaseClient client;
  
  private final Tsdb1xUniqueIdStore uid_store;
  
  private final boolean enable_appends;
  private final boolean enable_appends_coproc;
  
  public Tsdb1xHBaseDataStore(final Tsdb1xHBaseFactory factory,
                              final String id,
                              final Schema schema) {
    super(factory, id, schema);
    
    // TODO - flatten the config and pass it down to the client lib.
    final org.hbase.async.Config async_config = new org.hbase.async.Config();
    
    // We'll sync on the config object to avoid race conditions if 
    // multiple instances of this client are being loaded.
    final Configuration config = factory.tsdb().getConfig();
    synchronized(config) {
      // asynchbase flags
      if (!config.hasProperty(getConfigKey(ZK_QUORUM_KEY))) {
        config.register(getConfigKey(ZK_QUORUM_KEY), "localhost:2181", false, 
            "The comma separated list of Zookeeper servers and ports.");
      }
      if (!config.hasProperty(getConfigKey(ZNODE_PARENT_KEY))) {
        config.register(getConfigKey(ZNODE_PARENT_KEY), "/hbase", false, 
            "The base znode for HBase.");
      }
      if (!config.hasProperty(getConfigKey(AUTH_ENABLED_KEY))) {
        config.register(getConfigKey(AUTH_ENABLED_KEY), "false", false, 
            "Whether or not authentication is required to connect to "
            + "HBase region servers.");
      }
      if (!config.hasProperty(getConfigKey(KB_PRINCIPAL_KEY))) {
        config.register(getConfigKey(KB_PRINCIPAL_KEY), null, false, 
            "The principal template for kerberos authentication.");
      }
      if (!config.hasProperty(getConfigKey(KB_ENABLED_KEY))) {
        config.register(getConfigKey(KB_ENABLED_KEY), "false", false, 
            "Whether or not kerberos is enabled for authentication.");
      }
      if (!config.hasProperty(getConfigKey(SASL_CLIENT_KEY))) {
        config.register(getConfigKey(SASL_CLIENT_KEY), "Client", false, 
            "The SASL entry for the client in the JAAS config.");
      }
      if (!config.hasProperty(getConfigKey(META_SPLIT_KEY))) {
        config.register(getConfigKey(META_SPLIT_KEY), "false", false, 
            "Whether or not the meta table is split.");
      }
      
    }
    
    // copy into the asynchbase config.
    async_config.overrideConfig("hbase.zookeeper.quorum", 
        config.getString(getConfigKey(ZK_QUORUM_KEY)));
    async_config.overrideConfig("hbase.zookeeper.znode.parent", 
        config.getString(getConfigKey(ZNODE_PARENT_KEY)));
    async_config.overrideConfig("hbase.security.auth.enable", 
        config.getString(getConfigKey(AUTH_ENABLED_KEY)));
    async_config.overrideConfig("hbase.kerberos.regionserver.principal", 
        config.getString(getConfigKey(KB_PRINCIPAL_KEY)));
    if (config.getBoolean(getConfigKey(KB_ENABLED_KEY))) {
      async_config.overrideConfig("hbase.security.authentication", 
          "kerberos");
    }
    async_config.overrideConfig("hbase.sasl.clientconfig", 
        config.getString(getConfigKey(SASL_CLIENT_KEY)));
    async_config.overrideConfig("hbase.meta.split", 
        config.getString(getConfigKey(META_SPLIT_KEY)));
    
    enable_appends = config.getBoolean(ENABLE_APPENDS_KEY);
    enable_appends_coproc = config.getBoolean(ENABLE_COPROC_APPENDS_KEY);
    
    // TODO - shared client!
    client = new HBaseClient(async_config);
    
    // TODO - probably a better way. We may want to make the UniqueIdStore
    // it's own self-contained storage system.
    uid_store = new Tsdb1xUniqueIdStore(this);
    factory.tsdb().getRegistry().registerSharedObject(Strings.isNullOrEmpty(id) ? 
        "default_uidstore" : id + "_uidstore", uid_store);
  }
  
  @Override
  public QueryNode newNode(final QueryPipelineContext context,
                           final QueryNodeConfig config) {
    return new Tsdb1xHBaseQueryNode(factory, this, context, 
        (TimeSeriesDataSourceConfig) config);
  }
  
  @Override
  public Deferred<WriteStatus> write(final AuthState state, 
                                     final TimeSeriesDatum datum,
                                     final Span span) {
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
    
    class SuccessCB implements Callback<WriteStatus, Object> {
      @Override
      public WriteStatus call(final Object ignored) throws Exception {
        if (child != null) {
          child.setSuccessTags().finish();
        }
        return WriteStatus.OK;
      }
    }
    
    class WriteErrorCB implements Callback<WriteStatus, Exception> {
      @Override
      public WriteStatus call(final Exception ex) throws Exception {
        // TODO log?
        if (ex instanceof PleaseThrottleException ||
            ex instanceof RecoverableException) {
          if (child != null) {
            child.setErrorTags(ex)
                 .finish();
          }
          return WriteStatus.retry("Please retry at a later time.");
        }
        if (child != null) {
          child.setErrorTags(ex)
               .finish();
        }
        return WriteStatus.error(ex.getMessage(), ex);
      }
    }
    
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
            enable_appends || enable_appends_coproc, (int) base_time, null);
        
        if (enable_appends) {
          return client.append(new AppendRequest(data_table,ioe.id(), 
              DATA_FAMILY, pair.getKey(), pair.getValue()))
              .addCallbacks(new SuccessCB(), new WriteErrorCB());
        } else {
          // same for co-proc and puts. The encode method figures out
          // the qualifier and values.
          return client.put(new PutRequest(data_table, ioe.id(), 
              DATA_FAMILY, pair.getKey(), pair.getValue()))
              .addCallbacks(new SuccessCB(), new WriteErrorCB());
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
  public Deferred<List<WriteStatus>> write(final AuthState state,
                                           final TimeSeriesSharedTagsAndTimeData data, 
                                           final Span span) {
    // TODO Auto-generated method stub
    return null;
  }
  
  public Deferred<Object> shutdown() {
    // TODO - implement
    return Deferred.fromResult(null);
  }
  
  /** @return The HBase client. */
  HBaseClient client() {
    return client;
  }

  /** @return The UID store. */
  UniqueIdStore uidStore() {
    return uid_store;
  }
  
}

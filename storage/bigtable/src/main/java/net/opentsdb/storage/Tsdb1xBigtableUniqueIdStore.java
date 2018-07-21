// This file is part of OpenTSDB.
// Copyright (C) 2010-2018  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.LongAdder;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDatumId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.stats.Span;
import net.opentsdb.uid.IdOrError;
import net.opentsdb.uid.UniqueIdAssignmentAuthorizer;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

/**
 * Represents a table of Unique IDs, manages the lookup and creation of IDs.
 * <p>
 * Don't attempt to use {@code equals()} or {@code hashCode()} on
 * this class.
 * 
 * @since 1.0
 */
public class Tsdb1xBigtableUniqueIdStore implements UniqueIdStore {
  private static final Logger LOG = LoggerFactory.getLogger(
      Tsdb1xBigtableUniqueIdStore.class);

  /** A map of the various config UID types. */
  protected static final Map<UniqueIdType, String> CONFIG_PREFIX = 
      Maps.newHashMapWithExpectedSize(UniqueIdType.values().length);
  static {
    for (final UniqueIdType type : UniqueIdType.values()) {
      CONFIG_PREFIX.put(type, "uid." + type.toString().toLowerCase() + ".");
    }
  }
  
  /** The error message returned when an ID is being assigned in the
   * background and should be retried later. */
  public static final String ASSIGN_AND_RETRY = 
      "Assigning ID, queue and retry the data.";
  
  /** Various configuration keys. */
  public static final String CHARACTER_SET_KEY = "character_set";
  public static final String CHARACTER_SET_DEFAULT = "ISO-8859-1";
  public static final String ASSIGN_AND_RETRY_KEY = "assign_and_retry";
  public static final String RANDOM_ASSIGNMENT_KEY = "assign_random";
  public static final String RANDOM_ATTEMPTS_KEY = "attempts.max_random";
  public static final String ATTEMPTS_KEY = "attempts.max";
  
  public static final byte[] METRICS_QUAL = 
      "metrics".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_NAME_QUAL = 
      "tagk".getBytes(Const.ASCII_CHARSET);
  public static final byte[] TAG_VALUE_QUAL = 
      "tagv".getBytes(Const.ASCII_CHARSET);

  /** The single column family used by this class. */
  public static final byte[] ID_FAMILY = "id".getBytes(Const.ASCII_CHARSET);
  /** The single column family used by this class. */
  public static final byte[] NAME_FAMILY = "name".getBytes(Const.ASCII_CHARSET);
  /** Row key of the special row used to track the max ID already assigned. */
  public static final byte[] MAXID_ROW = { 0 };
  /** How many time do we try to assign an ID before giving up. */
  public static final short DEFAULT_ATTEMPTS_ASSIGN_ID = 3;
//  /** How many time do we try to apply an edit before giving up. */
//  public static final short MAX_ATTEMPTS_PUT = 6;
  /** How many time do we try to assign a random ID before giving up. */
  public static final short DEFAULT_ATTEMPTS_ASSIGN_RANDOM_ID = 10;
//  /** Initial delay in ms for exponential backoff to retry failed RPCs. */
//  private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
//  /** Maximum number of results to return in suggest(). */
//  private static final short MAX_SUGGESTIONS = 25;
  
  private final Tsdb1xBigtableDataStore data_store; 

  /** The authorizer pulled from the registry. */
  protected final UniqueIdAssignmentAuthorizer authorizer;
  
  /** Character sets for each type. */
  protected final Charset metric_character_set;
  protected final Charset tagk_character_set;
  protected final Charset tagv_character_set;

  /** Whether or not to immediately return a RETRY state and run the 
   * assignment process in the background. */
  protected final boolean assign_and_retry;
  
  /** Max attempts. */
  protected final short max_attempts_assign;
  protected final short max_attempts_assign_random;
  
  /** Whethe ror not to randomize UID assignments for these types. */
  protected final boolean randomize_metric_ids;
  protected final boolean randomize_tagk_ids;
  protected final boolean randomize_tagv_ids;
  
  private final Map<UniqueIdType, Map<String, Deferred<byte[]>>> pending_assignments;

  public Tsdb1xBigtableUniqueIdStore(final Tsdb1xBigtableDataStore data_store) {
    if (data_store == null) {
      throw new IllegalArgumentException("Data store cannot be null.");
    }
    this.data_store = data_store;
    registerConfigs(data_store.tsdb());
    
    authorizer = data_store.tsdb().getRegistry()
        .getDefaultPlugin(UniqueIdAssignmentAuthorizer.class);
    
    metric_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.METRIC) + CHARACTER_SET_KEY)));
    tagk_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.TAGK) + CHARACTER_SET_KEY)));
    tagv_character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.TAGV) + CHARACTER_SET_KEY)));
    
    randomize_metric_ids = data_store.tsdb().getConfig()
        .getBoolean(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.METRIC) + RANDOM_ASSIGNMENT_KEY));
    randomize_tagk_ids = data_store.tsdb().getConfig()
        .getBoolean(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.TAGK) + RANDOM_ASSIGNMENT_KEY));
    randomize_tagv_ids = data_store.tsdb().getConfig()
        .getBoolean(data_store.getConfigKey(
            CONFIG_PREFIX.get(UniqueIdType.TAGV) + RANDOM_ASSIGNMENT_KEY));
    
    assign_and_retry = data_store.tsdb().getConfig()
        .getBoolean(data_store.getConfigKey(ASSIGN_AND_RETRY_KEY));
    max_attempts_assign = (short) data_store.tsdb().getConfig()
        .getInt(data_store.getConfigKey(ATTEMPTS_KEY));
    max_attempts_assign_random = (short) data_store.tsdb().getConfig()
        .getInt(data_store.getConfigKey(RANDOM_ATTEMPTS_KEY));
    
    // It's important to create maps here.
    pending_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    for (final UniqueIdType type : UniqueIdType.values()) {
      pending_assignments.put(type, Maps.newConcurrentMap());
    }
    
    LOG.info("Initalized UniqueId store.");
  }

  @Override
  public Deferred<String> getName(final UniqueIdType type, 
                                  final byte[] id,
                                  final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Bytes.isNullOrEmpty(id)) {
      throw new IllegalArgumentException("ID cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getName")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("id", net.opentsdb.uid.UniqueId.uidToString(id))
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final Deferred<String> deferred = new Deferred<String>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uid_table))
        .setRows(RowSet.newBuilder()
            .addRowKeys(ByteStringer.wrap(id)))
        .setFilter(RowFilter.newBuilder()
            .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY))
            .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier))
            )
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        
        deferred.callback(results.isEmpty() ? null :
          new String(results.get(0).getFamilies(0).getColumns(0)
              .getCells(0).getValue().toByteArray(), characterSet(type)));
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch name.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), MoreExecutors.directExecutor());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  
  @Override
  public Deferred<List<String>> getNames(final UniqueIdType type, 
                                         final List<byte[]> ids,
                                         final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("IDs cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getNames")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          //.withTag("ids", /* TODO - an array to hex method */ "")
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    RowSet.Builder rows = RowSet.newBuilder();
    for (final byte[] id : ids) {
      if (Bytes.isNullOrEmpty(id)) {
        throw new IllegalArgumentException("A null or empty ID was "
            + "found in the list.");
      }
      rows.addRowKeys(ByteStringer.wrap(id));
    }
    
    final Deferred<List<String>> deferred = new Deferred<List<String>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uid_table))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY))
            .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier))
            )
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        
        final List<String> names = Lists.newArrayListWithCapacity(results.size());
        int id_idx = 0;
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getFamiliesCount() < 1 ||
              results.get(i).getFamilies(0).getColumnsCount() < 1) {
            continue;
          } else {
            while (Bytes.memcmp(results.get(i).getKey().toByteArray(), ids.get(id_idx++)) != 0) {
              names.add(null);
            }
            names.add(new String(results.get(i).getFamilies(0).getColumns(0)
                .getCells(0).getValue().toByteArray(), 
                characterSet(type)));
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        deferred.callback(names);
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch names.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), MoreExecutors.directExecutor());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }

  @Override
  public Deferred<byte[]> getId(final UniqueIdType type, 
                                final String name,
                                final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Name cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getId")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("name", name)
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    final Deferred<byte[]> deferred = new Deferred<byte[]>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uid_table))
        .setRows(RowSet.newBuilder()
            .addRowKeys(ByteStringer.wrap(name.getBytes(characterSet(type)))))
        .setFilter(RowFilter.newBuilder()
            .setFamilyNameRegexFilterBytes(ByteStringer.wrap(ID_FAMILY))
            .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier))
            )
        .build();
    
   class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        
        deferred.callback(results.isEmpty() ? null :
          results.get(0).getFamilies(0).getColumns(0).getCells(0)
            .getValue().toByteArray());
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch name.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), MoreExecutors.directExecutor());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  
  @Override
  public Deferred<List<byte[]>> getIds(final UniqueIdType type, 
                                       final List<String> names,
                                       final Span span) {
    if (type == null) {
      throw new IllegalArgumentException("Type cannot be null.");
    }
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException("Names cannot be null or empty.");
    }
    
    final Span child;
    if (span != null && span.isDebug()) {
      child = span.newChild(getClass().getName() + ".getIds")
          .withTag("dataStore", data_store.id())
          .withTag("type", type.toString())
          .withTag("names", names.toString())
          .start();
    } else {
      child = null;
    }
    
    final byte[] qualifier;
    switch(type) {
    case METRIC:
      qualifier = METRICS_QUAL;
      break;
    case TAGK:
      qualifier = TAG_NAME_QUAL;
      break;
    case TAGV:
      qualifier = TAG_VALUE_QUAL;
      break;
    default:
      throw new IllegalArgumentException("This data store does not "
          + "handle Unique IDs of type: " + type);
    }
    
    RowSet.Builder rows = RowSet.newBuilder();
    for (final String name : names) {
      if (Strings.isNullOrEmpty(name)) {
        throw new IllegalArgumentException("A null or empty name was "
            + "found in the list.");
      }
      rows.addRowKeys(ByteStringer.wrap(name.getBytes(characterSet(type))));
    }
    
    final Deferred<List<byte[]>> deferred = new Deferred<List<byte[]>>();
    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setTableNameBytes(ByteStringer.wrap(data_store.uid_table))
        .setRows(rows.build())
        .setFilter(RowFilter.newBuilder()
            .setFamilyNameRegexFilterBytes(ByteStringer.wrap(NAME_FAMILY))
            .setColumnQualifierRegexFilter(ByteStringer.wrap(qualifier))
            )
        .build();
    
    class ResultCB implements FutureCallback<List<Row>> {

      @Override
      public void onSuccess(final List<Row> results) {
        if (results == null) {
          throw new StorageException("Result list returned was null");
        }
        
        final List<byte[]> ids = Lists.newArrayListWithCapacity(results.size());
        int name_idx = 0;
        for (int i = 0; i < results.size(); i++) {
          if (results.get(i).getFamiliesCount() < 1 ||
              results.get(i).getFamilies(0).getColumnsCount() < 1) {
            continue;
          } else {
            while (!new String(results.get(i).getKey().toByteArray(), characterSet(type))
                .equals(names.get(name_idx++))) {
              ids.add(null);
            }
            ids.add(results.get(i).getFamilies(0).getColumns(0)
                .getCells(0).getValue().toByteArray());
          }
        }
        
        if (child != null) {
          child.setSuccessTags()
            .finish();
        }
        deferred.callback(names);
      }

      @Override
      public void onFailure(final Throwable t) {
        if (child != null) {
          child.setErrorTags()
            .log("Exception", t)
            .finish();
        }
        deferred.callback(new StorageException("Failed to fetch id.", t));
      }
      
    }
    
    try {
      Futures.addCallback(data_store.executor().readRowsAsync(request), 
          new ResultCB(), MoreExecutors.directExecutor());
    } catch (Exception e) {
      if (child != null) {
        child.setErrorTags()
          .log("Exception", e)
          .finish();
      }
      return Deferred.fromError(new StorageException(
          "Unexpected exception from storage", e));
    }
    return deferred;
  }
  

  @Override
  public Deferred<IdOrError> getOrCreateId(AuthState auth, UniqueIdType type,
      String name, TimeSeriesDatumId id, Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<IdOrError>> getOrCreateIds(AuthState auth,
      UniqueIdType type, List<String> names, TimeSeriesDatumId id, Span span) {
    
    return null;
  }


  @Override
  public Charset characterSet(final UniqueIdType type) {
    switch (type) {
    case METRIC:
      return metric_character_set;
    case TAGK:
      return tagk_character_set;
    case TAGV:
      return tagv_character_set;
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  @VisibleForTesting
  void registerConfigs(final TSDB tsdb) {
    for (final Entry<UniqueIdType, String> entry : CONFIG_PREFIX.entrySet()) {
      if (!data_store.tsdb().getConfig().hasProperty(
          data_store.getConfigKey(entry.getValue() + CHARACTER_SET_KEY))) {
        data_store.tsdb().getConfig().register(
            data_store.getConfigKey(entry.getValue() + CHARACTER_SET_KEY), 
            CHARACTER_SET_DEFAULT, 
            false, 
            "The character set used for encoding/decoding UID "
            + "strings for " + entry.getKey().toString().toLowerCase() 
            + " entries.");
      }
      
      if (!data_store.tsdb().getConfig().hasProperty(
          data_store.getConfigKey(entry.getValue() + RANDOM_ASSIGNMENT_KEY))) {
        data_store.tsdb().getConfig().register(
            data_store.getConfigKey(entry.getValue() + RANDOM_ASSIGNMENT_KEY), 
            false, 
            false, 
            "Whether or not to randomly assign UIDs for " 
            + entry.getKey().toString().toLowerCase() + " entries "
                + "instead of incrementing a counter in storage.");
      }
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        data_store.getConfigKey(ASSIGN_AND_RETRY_KEY))) {
      data_store.tsdb().getConfig().register(
          data_store.getConfigKey(ASSIGN_AND_RETRY_KEY), 
          false, 
          false, 
          "Whether or not to return with a RETRY write state immediately "
          + "when a UID needs to be assigned and being the assignment "
          + "process in the background. The next time the same string "
          + "is looked up it should be assigned.");
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        data_store.getConfigKey(RANDOM_ATTEMPTS_KEY))) {
      data_store.tsdb().getConfig().register(
          data_store.getConfigKey(RANDOM_ATTEMPTS_KEY), 
          DEFAULT_ATTEMPTS_ASSIGN_RANDOM_ID, 
          false, 
          "The maximum number of attempts to make before giving up on "
          + "a UID assignment and return a RETRY error when in the "
          + "random ID mode. (This is usually higher than the normal "
          + "mode as random IDs can collide more often.)");
    }
    
    if (!data_store.tsdb().getConfig().hasProperty(
        data_store.getConfigKey(ATTEMPTS_KEY))) {
      data_store.tsdb().getConfig().register(
          data_store.getConfigKey(ATTEMPTS_KEY), 
          DEFAULT_ATTEMPTS_ASSIGN_ID, 
          false, 
          "The maximum number of attempts to make before giving up on "
          + "a UID assignment and return a RETRY error.");
    }
  }
  
}

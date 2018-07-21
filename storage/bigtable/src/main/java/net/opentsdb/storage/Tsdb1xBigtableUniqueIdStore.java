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
import java.util.concurrent.atomic.LongAdder;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.stats.Span;
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
  
  public static final String CHARACTER_SET_KEY = "character_set";
  public static final String CHARACTER_SET_DEFAULT = "ISO-8859-1";
  
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
  private final Tsdb1xBigtableDataStore data_store; 
  
  private final Charset character_set;
  
  private final Map<UniqueIdType, LongAdder> rejected_assignments;
  
  private final Map<UniqueIdType, Map<String, Deferred<byte[]>>> pending_assignments;

  public Tsdb1xBigtableUniqueIdStore(final Tsdb1xBigtableDataStore data_store) {
    this.data_store = data_store;
    if (!data_store.tsdb().getConfig().hasProperty(
        data_store.getConfigKey(CHARACTER_SET_KEY))) {
      data_store.tsdb().getConfig().register(
          data_store.getConfigKey(CHARACTER_SET_KEY), 
          CHARACTER_SET_DEFAULT, 
          false, 
          "The character set used for encoding/decoding all strings "
              + "to UIDs.");
    }
    character_set = Charset.forName(data_store.tsdb().getConfig()
        .getString(data_store.getConfigKey(CHARACTER_SET_KEY)));
    
    rejected_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    pending_assignments = Maps.newHashMapWithExpectedSize(
        UniqueIdType.values().length);
    
    for (final UniqueIdType type : UniqueIdType.values()) {
      rejected_assignments.put(type, new LongAdder());
      pending_assignments.put(type, Maps.newConcurrentMap());
    }
//    this.client = client;
//    this.table = table;
//    if (kind.isEmpty()) {
//      throw new IllegalArgumentException("Empty string as 'kind' argument!");
//    }
//    this.kind = toBytes(kind);
//    type = stringToUniqueIdType(kind);
//    if (width < 1 || width > 8) {
//      throw new IllegalArgumentException("Invalid width: " + width);
//    }
//    this.id_width = (short) width;
//    this.randomize_id = randomize_id;
    
    LOG.info("Initalized UniqueId store with characterset: " 
        + character_set);
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
              .getCells(0).getValue().toByteArray(), character_set));
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
                character_set));
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
            .addRowKeys(ByteStringer.wrap(name.getBytes(character_set))))
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
      rows.addRowKeys(ByteStringer.wrap(name.getBytes(character_set)));
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
            while (!new String(results.get(i).getKey().toByteArray(), character_set)
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
  public Deferred<byte[]> getOrCreateId(final UniqueIdType type, 
                                        final String name,
                                        final TimeSeriesId id,
                                        final Span span) {
    // TODO - implement
    return null;
//    if (type == null) {
//      throw new IllegalArgumentException("Type cannot be null.");
//    }
//    if (Strings.isNullOrEmpty(name)) {
//      throw new IllegalArgumentException("Name cannot be null or empty.");
//    }
//    if (id == null) {
//      throw new IllegalArgumentException("The ID cannot be null.");
//    }
//    
//    final Span child;
//    if (span != null && span.isDebug()) {
//      child = span.newChild("Tsdb1x_getIds")
//          .withTag("dataStore", data_store.id())
//          .withTag("type", type.toString())
//          .withTag("name", name)
//          .withTag("id", id.toString())
//          .start();
//    } else {
//      child = null;
//    }
//    
//    /** Triggers the assignment if allowed through the filter */
//    class AssignmentAllowedCB implements  Callback<Deferred<byte[]>, Boolean> {
//      @Override
//      public Deferred<byte[]> call(final Boolean allowed) throws Exception {
//        if (!allowed) {
//          rejected_assignments.get(type).increment();
//          return Deferred.fromError(new FailedToAssignUniqueIdException(
//              type, name, 0, "Blocked by UID filter."));
//        }
//        
//        Deferred<byte[]> assignment = null;
//        synchronized (pending_assignments) {
//          assignment = pending_assignments.get(name);
//          if (assignment == null) {
//            // to prevent UID leaks that can be caused when multiple time
//            // series for the same metric or tags arrive, we need to write a 
//            // deferred to the pending map as quickly as possible. Then we can 
//            // start the assignment process after we've stashed the deferred 
//            // and released the lock
//            assignment = new Deferred<byte[]>();
//            pending_assignments.put(name, assignment);
//          } else {
//            LOG.info("Already waiting for UID assignment: " + name);
//            return assignment;
//          }
//        }
//        
//        // start the assignment dance after stashing the deferred
//        if (metric != null && LOG.isDebugEnabled()) {
//          LOG.debug("Assigning UID for '" + name + "' of type '" + type + 
//              "' for series '" + metric + ", " + tags + "'");
//        }
//        
//        // start the assignment dance after stashing the deferred
//        return new UniqueIdAllocator(name, assignment).tryAllocate();
//      }
//      @Override
//      public String toString() {
//        return "AssignmentAllowedCB";
//      }
//    }
//    
//    /** Triggers an assignment (possibly through the filter) if the exception 
//     * returned was a NoSuchUniqueName. */
//    class HandleNoSuchUniqueNameCB implements Callback<Object, Exception> {
//      public Object call(final Exception e) {
//        if (e instanceof NoSuchUniqueName) {
//          if (tsdb != null && tsdb.getUidFilter() != null && 
//              tsdb.getUidFilter().fillterUIDAssignments()) {
//            return tsdb.getUidFilter()
//                .allowUIDAssignment(type, name, metric, tags)
//                .addCallbackDeferring(new AssignmentAllowedCB());
//          } else {
//            return Deferred.fromResult(true)
//                .addCallbackDeferring(new AssignmentAllowedCB());
//          }
//        }
//        return e;  // Other unexpected exception, let it bubble up.
//      }
//    }
//
//    // Kick off the HBase lookup, and if we don't find it there either, start
//    // the process to allocate a UID.
//    return getId(name).addErrback(new HandleNoSuchUniqueNameCB());
  }

  @Override
  public Deferred<List<byte[]>> getOrCreateIds(final UniqueIdType type, 
                                               final List<String> names,
                                               final TimeSeriesId id,
                                               final Span span) {
    // TODO - implement
    return null;
  }
  
  @Override
  public Charset characterSet() {
    return character_set;
  }
  
}

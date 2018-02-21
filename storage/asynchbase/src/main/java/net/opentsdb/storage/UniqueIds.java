package net.opentsdb.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.storage.schemas.v1.V1Schema;
import net.opentsdb.uid.LRUUniqueId;
import net.opentsdb.uid.ResolvedFilter;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.uid.UniqueIdStore;

public class UniqueIds implements UniqueIdStore {

  private static final String METRICS_QUAL = "metrics";
  private static final String TAG_NAME_QUAL = "tagk";
  private static final String TAG_VALUE_QUAL = "tagv";
  
  private final V1AsyncHBaseDataStore store;
  
  private final HBaseClient client;
  
  private final byte[] table;
  private final byte[] id_family;
  private final byte[] name_family;
  
  public UniqueIds(final V1AsyncHBaseDataStore store, 
      final byte[] table, final byte[] id_family, final byte[] name_family) {
    this.store = store;
    this.client = store.client();
    this.table = table;
    this.id_family = id_family;
    this.name_family = name_family;
  }
  
  Deferred<Object> validateTables() {
    // TODO - implement!
    return Deferred.fromError(new UnsupportedOperationException("GRRR!!!"));
  }

  @Override
  public Deferred<byte[]> stringToId(UniqueIdType type, String id) {
    final GetRequest get = new GetRequest(
        table,
        id.getBytes(Const.ASCII_CHARSET /* TODO - proper config */), 
        id_family); // TODO - make sure this is right, may have it backy
    switch (type) {
    case METRIC:
      get.qualifier(METRICS_QUAL);
      break;
    case TAGK:
      get.qualifier(TAG_NAME_QUAL);
      break;
    case TAGV:
      get.qualifier(TAG_VALUE_QUAL);
      break;
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
    
    class GetCB implements Callback<byte[], ArrayList<KeyValue>> {
      public byte[] call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        return row.get(0).value();
      }
    }
    return client.get(get).addCallback(new GetCB());
  }

  @Override
  public Deferred<String> idToString(UniqueIdType type, byte[] id) {
    final GetRequest get = new GetRequest(
        table, 
        id, 
        name_family); // TODO - make sure this is right, may have it backy
    switch (type) {
    case METRIC:
      get.qualifier(METRICS_QUAL);
      break;
    case TAGK:
      get.qualifier(TAG_NAME_QUAL);
      break;
    case TAGV:
      get.qualifier(TAG_VALUE_QUAL);
      break;
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
    
    class GetCB implements Callback<String, ArrayList<KeyValue>> {
      public String call(final ArrayList<KeyValue> row) {
        if (row == null || row.isEmpty()) {
          return null;
        }
        // TODO - proper config
        return new String(row.get(0).value(), Const.ASCII_CHARSET);
      }
    }
    return client.get(get).addCallback(new GetCB());
  }

  @Override
  public Deferred<List<byte[]>> stringsToId(UniqueIdType type, List<String> ids) {
    // TODO - proper multiget. This is... ugly and wrong
    final List<Deferred<Object>> gets = Lists.newArrayListWithCapacity(ids.size());
    final List<byte[]> results = Lists.newArrayListWithCapacity(ids.size());
    for (int i = 0; i < ids.size(); i++) {
      results.add(null);
    }
    
    class FinalCB implements Callback<Deferred<List<byte[]>>, ArrayList<Object>> {
      @Override
      public Deferred<List<byte[]>> call(final ArrayList<Object> ignored) throws Exception {
        return Deferred.fromResult(results);
      }
    }
    
    class JoinerCB implements Callback<Object, byte[]> {
      final int idx;
     
      JoinerCB(final int idx) {
        this.idx = idx;
      }

      @Override
      public Object call(final byte[] uid) throws Exception {
        synchronized(results) {
          results.set(idx, uid);
        }
        return null;
      }
      
    }
    
    for (int i = 0; i < ids.size(); i++) {
      final String name = ids.get(i);
      if (name == null) {
        continue;
      }
      
      gets.add(stringToId(type, name).addCallback(new JoinerCB(i)));
    }
    
    return Deferred.group(gets).addCallbackDeferring(new FinalCB());
  }

  @Override
  public Deferred<List<String>> idsToString(UniqueIdType type, List<byte[]> ids) {
    // TODO - proper multiget. This is... ugly and wrong
    final List<Deferred<Object>> gets = Lists.newArrayListWithCapacity(ids.size());
    final List<String> results = Lists.newArrayListWithCapacity(ids.size());
    for (int i = 0; i < ids.size(); i++) {
      results.add(null);
    }
    
    class FinalCB implements Callback<Deferred<List<String>>, ArrayList<Object>> {
      @Override
      public Deferred<List<String>> call(final ArrayList<Object> ignored) throws Exception {
        return Deferred.fromResult(results);
      }
    }
    
    class JoinerCB implements Callback<Object, String> {
      final int idx;
     
      JoinerCB(final int idx) {
        this.idx = idx;
      }

      @Override
      public Object call(final String name) throws Exception {
        synchronized(results) {
          results.set(idx, name);
        }
        return null;
      }
      
    }
    
    for (int i = 0; i < ids.size(); i++) {
      final byte[] uid = ids.get(i);
      if (uid == null) {
        continue;
      }
      
      gets.add(idToString(type, uid).addCallback(new JoinerCB(i)));
    }
    
    return Deferred.group(gets).addCallbackDeferring(new FinalCB());
  }

}

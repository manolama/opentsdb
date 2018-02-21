package net.opentsdb.uid;

import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.stats.TsdbTrace;

public class LRUUniqueId extends UniqueId {

  /** Cache for forward mappings (name to ID). */
  private final Cache<String, byte[]> name_cache;
  
  /** Cache for backward mappings (ID to name).
   * The ID in the key is a byte[] converted to a String to be Comparable. */
  private final Cache<String, String> id_cache;
  
  public LRUUniqueId(TSDB tsdb, UniqueIdConfig config, UniqueIdStore store) {
    super(tsdb, config, store);
    
    // TODO - only one or the other depending on mode
    name_cache = CacheBuilder.newBuilder()
        .maximumSize(1024 /*tsdb.getConfig().getInt("tsd.uid." + config.type().toString().toLowerCase() + ".lru.name.size")*/)
        .build();
    id_cache = CacheBuilder.newBuilder()
        .maximumSize(1024/*tsdb.getConfig().getInt("tsd.uid." + config.type().toString().toLowerCase() + "lru.id.size")*/)
        .build();
  }

  @Override
  public void dropCaches() {
    // TODO - only one or the other depending on mode
    name_cache.invalidateAll();
    id_cache.invalidateAll();
  }

  @Override
  public boolean throwsNoSuchUniques() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Deferred<String> getName(byte[] id, TsdbTrace trace, Span span) {
    final String name = id_cache.getIfPresent(fromBytes(id));
    if (name != null) {
      return Deferred.fromResult(name);
    }
    return store.idToString(config.type(), id)
        .addCallbackDeferring(new IdToString(id));
  }

  @Override
  public Deferred<List<String>> getName(List<byte[]> ids, TsdbTrace trace,
      Span span) {
    // need the clone so we can dump cached copies
    final List<byte[]> clone = Lists.newArrayList(ids);
    final List<String> names = Lists.newArrayListWithCapacity(ids.size());
    for (int i = 0; i < ids.size(); i++) {
      names.add(null);
    }
    int resolved = 0;
    for (int i = 0; i < ids.size(); i++) {
      final String name = id_cache.getIfPresent(fromBytes(ids.get(i)));
      if (name != null) {
        clone.set(i, null);
        names.set(i, name);
        resolved++;
      }
    }
    
    if (resolved == ids.size()) {
      return Deferred.fromResult(names);
    }
    
    return store.idsToString(config.type(), clone)
        .addCallbackDeferring(new IdsToStrings(ids, names));
  }

  @Override
  public Deferred<byte[]> getId(String name, TsdbTrace trace, Span span) {
    final byte[] uid = name_cache.getIfPresent(name);
    if (uid != null) {
      return Deferred.fromResult(uid);
    }
    return store.stringToId(config.type(), name)
        .addCallbackDeferring(new StringToId(name));
  }

  @Override
  public Deferred<List<byte[]>> getId(List<String> names, TsdbTrace trace,
      Span span) {
    // need the clone so we can dump cached copies
    final List<String> clone = Lists.newArrayList(names);
    final List<byte[]> uids = Lists.newArrayListWithCapacity(names.size());
    int resolved = 0;
    for (int i = 0; i < names.size(); i++) {
      final byte[] uid = name_cache.getIfPresent(names.get(i));
      if (uid != null) {
        clone.set(i, null);
        uids.set(i, uid);
        resolved++;
      }
    }
    
    if (resolved == names.size()) {
      return Deferred.fromResult(uids);
    }
    
    return store.stringsToId(config.type(), clone)
        .addCallbackDeferring(new StringsToIds(clone, uids));
  }

  @Override
  public Deferred<byte[]> getOrCreateId(String name, TimeSeriesStringId id,
      TsdbTrace trace, Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<byte[][]> getOrCreateId(List<String> names,
      List<TimeSeriesStringId> ids, TsdbTrace trace, Span span) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<List<String>> suggest(String search, int max_results) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> rename(String oldname, String newname) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> deleteAsync(String name) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

  private class IdToString implements Callback<Deferred<String>, String> {
    final byte[] id;
    
    IdToString(final byte[] id) {
      this.id = id;
    }
    
    @Override
    public Deferred<String> call(final String name) throws Exception {
      switch (config.mode()) {
      case WRITE_ONLY:
        break;
      case READ_ONLY:
        id_cache.put(fromBytes(id), name);
        break;
      default:
        id_cache.put(fromBytes(id), name);
        name_cache.put(name, id);
      }
      return Deferred.fromResult(name);
    }
    
  }
  
  private class IdsToStrings implements Callback<Deferred<List<String>>, List<String>> {
    final List<byte[]> ids;
    final List<String> cached;
    
    IdsToStrings(final List<byte[]> ids, final List<String> cached) {
      this.ids = ids;
      this.cached = cached;
    }
    
    @Override
    public Deferred<List<String>> call(final List<String> names) throws Exception {
      switch (config.mode()) {
      case WRITE_ONLY:
        break;
      case READ_ONLY:
        for (int i = 0; i < names.size(); i++) {
          final String name = names.get(i);
          if (name != null) {
            id_cache.put(fromBytes(ids.get(i)), name);
            cached.set(i, name);
          }
        }
        break;
      default:
        for (int i = 0; i < names.size(); i++) {
          final String name = names.get(i);
          if (name != null) {
            id_cache.put(fromBytes(ids.get(i)), name);
            name_cache.put(name, ids.get(i));
            cached.set(i, name);
          }
        }
      }
      return Deferred.fromResult(cached);
    }
  }

  private class StringToId implements Callback<Deferred<byte[]>, byte[]> {
    final String name;
    
    StringToId(final String name) {
      this.name = name;
    }
    
    @Override
    public Deferred<byte[]> call(final byte[] uid) throws Exception {
      switch (config.mode()) {
      case WRITE_ONLY:
        name_cache.put(name, uid);
        break;
      case READ_ONLY:
        break;
      default:
        id_cache.put(fromBytes(uid), name);
        name_cache.put(name, uid);
      }
      return Deferred.fromResult(uid);
    }
    
  }
  
  private class StringsToIds implements Callback<Deferred<List<byte[]>>, List<byte[]>> {
    final List<String> names;
    final List<byte[]> cached;
    
    StringsToIds(final List<String> names, final List<byte[]> cached) {
      this.names = names;
      this.cached = cached;
    }
    
    @Override
    public Deferred<List<byte[]>> call(final List<byte[]> uids) throws Exception {
      switch (config.mode()) {
      case WRITE_ONLY:
        for (int i = 0; i < uids.size(); i++) {
          final byte[] uid = uids.get(i);
          if (uid != null) {
            name_cache.put(names.get(i), uid);
            cached.set(i, uid);
          }
        }
        break;
      case READ_ONLY:
        break;
      default:
        for (int i = 0; i < names.size(); i++) {
          final byte[] uid = uids.get(i);
          if (uid != null) {
            id_cache.put(fromBytes(uid), names.get(i));
            name_cache.put(names.get(i), uid);
            cached.set(i, uid);
          }
        }
      }
      return Deferred.fromResult(cached);
    }
    
  }
}

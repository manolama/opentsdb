package net.opentsdb.storage.schemas.v1;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVLiteralOrFilter;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.storage.UidSchema;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.uid.UniqueIdConfig.Mode;
import net.opentsdb.uid.UniqueIdStore;
import net.opentsdb.utils.Bytes;
import net.opentsdb.uid.LRUUniqueId;
import net.opentsdb.uid.ResolvedFilter;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdConfig;

public class V1Schema implements UidSchema {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  
  V1NumericCodec numeric_codec = new V1NumericCodec();
  
  private UniqueId metrics;
  
  private UniqueId tag_keys;
  
  private UniqueId tag_values;
  
  private UniqueIdStore store;
  
  public V1Schema(final TSDB tsdb, final UniqueIdStore store) {
    this.store = store;
    metrics = new LRUUniqueId(tsdb, new UniqueIdConfig(Mode.READ_WRITE, UniqueIdType.METRIC, 3), store);
    tag_keys = new LRUUniqueId(tsdb, new UniqueIdConfig(Mode.READ_WRITE, UniqueIdType.TAGK, 3), store);
    tag_values = new LRUUniqueId(tsdb, new UniqueIdConfig(Mode.READ_WRITE, UniqueIdType.TAGV, 3), store);
  }
  
  @Override
  public byte[] timelessKey(final byte[] key) {
    int offset = saltWidth() + 3 /* TODO - fix */ + Const.TIMESTAMP_BYTES;
    final byte[] timeless = new byte[key.length - saltWidth() - Const.TIMESTAMP_BYTES];
    System.arraycopy(key, saltWidth(), timeless, 0, 3 /* TODO - fix */);
    System.arraycopy(key, offset, timeless, 3, /* TODO - fix */ key.length - offset);
    return timeless;
  }

  @Override
  public TimeStamp baseTimestamp(byte[] key) {
    // TODO Auto-generated method stub
    return new MillisecondTimeStamp(Bytes.getUnsignedInt(key, saltWidth() + 3 /* TODO - fix */));
  }

  public TypeToken<?> prefixToType(final byte prefix) {
    // TODO - fix
    return NumericType.TYPE;
  }
  
  public Map<TypeToken<?>, Byte> typeToPrefix() {
    // TODO implement
    return null;
  }
  
  public V1Codec getCodec(final byte prefix) {
    // TODO implement
    return numeric_codec;
  }
  
  public V1Codec getCodec(final TypeToken<?> type) {
    // TODO implement
    return null;
  }
  
  public int saltBuckets() {
    // TODO - implement
    return 0;
  }
  
  public int saltWidth() {
    // TODO - implement
    return 0;
  }
  
  public int metricUIDBytes() {
    // TODO - implement
    return 3;
  }
  
  public int tagKUIDBytes() {
    // TODO - implement
    return 3;
  }
  
  public int tagVUIDBytes() {
    // TODO - implement
    return 3;
  }
  
  public RollupConfig rollupConfig() {
    // TODO - implement
    return null;
  }
  
  public long rowKeyTimestampMask()  {
    // TODO - implement
    return 0;
  }
  
  public long alignDownsamplerBaseTimestamp(final Downsampler downsampler, final long query_ts) {
    // TODO - implement
    // account for: calendar and row width
    return 0;
  }
  
  public long alignDownsamplerNextTimestamp(final Downsampler downsampler, final long query_ts) {
    // TODO - implement
    // account for: calendar and row width
    return 0;
  }
  
  public long alignQueryBaseTimestamp(final long query_ts) {
    // TODO - implement
    final long timespan_offset = query_ts % Const.MAX_TIMESPAN;
    return query_ts - timespan_offset;
  }
  
  public long alignQueryNextTimestamp(final long query_ts) {
    // TODO - implement
    final long timespan_offset = query_ts % Const.MAX_TIMESPAN;
    final long timespan_aligned_ts = query_ts - timespan_offset;
    return timespan_aligned_ts + Const.MAX_TIMESPAN;
  }
  
  public void setSaltBucket(final int bucket, final byte[] row_key) {
    // TODO - implement
  }
  

  @Override
  public int uidWidth(UniqueIdType type) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Deferred<byte[]> stringToId(UniqueIdType type, String id) {
    switch (type) {
    case METRIC:
      return metrics.getId(id);
    case TAGK:
      return tag_keys.getId(id);
    case TAGV:
      return tag_values.getId(id);
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
  }

  @Override
  public Deferred<List<byte[]>> stringsToId(UniqueIdType type,
      List<String> ids) {
    switch (type) {
    case METRIC:
      return metrics.getId(ids);
    case TAGK:
      return tag_keys.getId(ids);
    case TAGV:
      return tag_values.getId(ids);
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
  }

  @Override
  public Deferred<String> idToString(UniqueIdType type, byte[] id) {
    switch (type) {
    case METRIC:
      return metrics.getName(id);
    case TAGK:
      return tag_keys.getName(id);
    case TAGV:
      return tag_values.getName(id);
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
  }

  @Override
  public Deferred<List<String>> idsToString(UniqueIdType type,
      List<byte[]> ids) {
    switch (type) {
    case METRIC:
      return metrics.getName(ids);
    case TAGK:
      return tag_keys.getName(ids);
    case TAGV:
      return tag_values.getName(ids);
    default:
      throw new RuntimeException("WTF!~?!?!?!?!?"); // TODO - proper
    }
  }

  public Deferred<List<ResolvedFilter>> resolveUids(final Filter filter) {
    System.out.println("RESOLVING FILTER: " + filter);
    final List<ResolvedFilter> resolved_filters = 
        Lists.newArrayListWithCapacity(filter.getTags().size());
    for (int i = 0; i < filter.getTags().size(); i++) {
      resolved_filters.add(null);
    }
    
    class TagVCB implements Callback<Object, List<byte[]>> {
      final int idx;
      
      TagVCB(final int idx) {
        this.idx = idx;
      }

      @Override
      public Object call(final List<byte[]> uids) throws Exception {
        // TODO - may need sync
        resolved_filters.get(idx).setTagValues(uids);
        return null;
      }
      
    }
    
    class TagKCB implements Callback<Deferred<Object>, byte[]> {
      final int idx;
      final TagVFilter f;
      
      TagKCB(final int idx, final TagVFilter f) {
        this.idx = idx;
        this.f = f;
      }

      @Override
      public Deferred<Object> call(final byte[] uid) throws Exception {
        final ResolvedFilter resolved = new ResolvedFilter();
        resolved.setTagKey(uid);
        resolved_filters.set(idx, resolved); // TODO - wonder if we need sync here since it's an array
        final List<String> tags = Lists.newArrayList(((TagVLiteralOrFilter) f).literals());
        return tag_values.getId(tags, (TsdbTrace) null, null /* TOD - setem */)
            .addCallback(new TagVCB(idx));
      }
      
    }
    
    List<Deferred<Object>> deferreds = Lists.newArrayListWithCapacity(filter.getTags().size());
    for (int i = 0; i < filter.getTags().size(); i++) {
      final TagVFilter f = filter.getTags().get(i);
      if (f instanceof TagVLiteralOrFilter) {
        deferreds.add(tag_keys.getId(f.getTagk())
            .addCallbackDeferring(new TagKCB(i, f)));
      }
    }
    
    class FinalCB implements Callback<List<ResolvedFilter>, ArrayList<Object>> {

      @Override
      public List<ResolvedFilter> call(final ArrayList<Object> ignored)
          throws Exception {
        System.out.println("DONE WITH RESOLVE FILTERS");
        return resolved_filters;
      }
      
    }
    
    return Deferred.group(deferreds).addCallback(new FinalCB());
  }
}

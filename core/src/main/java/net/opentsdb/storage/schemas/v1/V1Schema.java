package net.opentsdb.storage.schemas.v1;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.storage.UidSchema;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.uid.UniqueIdConfig.Mode;
import net.opentsdb.utils.Bytes;
import net.opentsdb.uid.UniqueIdConfig;

public class V1Schema implements UidSchema {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  
  V1NumericCodec numeric_codec = new V1NumericCodec();
  
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
  
  public UniqueIdConfig getConfig(final UniqueIdType type) {
    final UniqueIdConfig conf = new UniqueIdConfig();
    conf.type = type;
    conf.mode = Mode.READ_WRITE;
    conf.width = 3;
    
    return conf;
  }
}

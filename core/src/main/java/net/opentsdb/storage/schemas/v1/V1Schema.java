package net.opentsdb.storage.schemas.v1;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.Const;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.storage.UidSchema;

public class V1Schema implements UidSchema {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;
  
  @Override
  public byte[] timelessKey(final byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TimeStamp baseTimestamp(byte[] key) {
    // TODO Auto-generated method stub
    return null;
  }

  public TypeToken<?> prefixToType(final byte prefix) {
    return null;
  }
  
  public Map<TypeToken<?>, Byte> typeToPrefix() {
    return null;
  }
  
  public V1Codec getCodec(final byte prefix) {
    return null;
  }
  
  public V1Codec getCodec(final TypeToken<?> type) {
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
    return 0;
  }
  
  public int tagKUIDBytes() {
    // TODO - implement
    return 0;
  }
  
  public int tagVUIDBytes() {
    // TODO - implement
    return 0;
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
    //final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
    //final long timespan_aligned_ts = interval_aligned_ts - timespan_offset;
    return 0;
  }
  
  public long alignQueryNextTimestamp(final long query_ts) {
    // TODO - implement
    //final long timespan_offset = interval_aligned_ts % Const.MAX_TIMESPAN;
    //final long timespan_aligned_ts = interval_aligned_ts - timespan_offset;
    return 0;
  }
  
  public void setSaltBucket(final int bucket, final byte[] row_key) {
    // TODO - implement
  }
}

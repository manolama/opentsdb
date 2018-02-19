package net.opentsdb.storage.schemas.v1;

import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.storage.UidSchema;

public class V1Schema implements UidSchema {

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
    return 0;
  }
  
  public int saltWidth() {
    return 0;
  }
  
  public int metricUIDBytes() {
    return 0;
  }
  
  public int tagKUIDBytes() {
    return 0;
  }
  
  public int tagVUIDBytes() {
    return 0;
  }
}

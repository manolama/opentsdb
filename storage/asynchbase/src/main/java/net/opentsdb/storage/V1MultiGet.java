package net.opentsdb.storage;

import java.util.List;

import net.opentsdb.meta.V1MetaResult;
import net.opentsdb.storage.schemas.v1.V1Result;

public class V1MultiGet {

  private final V1QueryNode node;
  private final V1MetaResult meta_result;
  
  public V1MultiGet(final V1QueryNode node, final V1MetaResult meta_result) {
    this.node = node;
    this.meta_result = meta_result;
  }
  
  public void fetchNext(final V1Result result) {
    
  }
  
  public StorageState state() {
    return null;
  }
}

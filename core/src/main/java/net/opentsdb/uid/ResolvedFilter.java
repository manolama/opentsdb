package net.opentsdb.uid;

import java.util.List;

public class ResolvedFilter {

  private byte[] tag_key;
  
  private List<byte[]> tag_values;
  
  public byte[] getTagKey() {
    return tag_key;
  }

  public void setTagKey(byte[] tag_key) {
    this.tag_key = tag_key;
  }

  public List<byte[]> getTagValues() {
    return tag_values;
  }

  public void setTagValues(final List<byte[]> tag_values) {
    this.tag_values = tag_values;
  }  
}

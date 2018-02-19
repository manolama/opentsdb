package net.opentsdb.uid;

public class ResolvedFilter {

  private byte[] metric;
  
  private byte[] tag_key;
  
  private byte[] tag_values;
  
  public byte[] getMetric() {
    return metric;
  }

  public void setMetric(byte[] metric) {
    this.metric = metric;
  }

  public byte[] getTagKey() {
    return tag_key;
  }

  public void setTagKey(byte[] tag_key) {
    this.tag_key = tag_key;
  }

  public byte[] getTagValues() {
    return tag_values;
  }

  public void setTagValues(byte[] tag_values) {
    this.tag_values = tag_values;
  }  
}

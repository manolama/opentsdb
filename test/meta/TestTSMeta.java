package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Test;

public final class TestTSMeta {
  TSMeta meta = new TSMeta();
  
  @Test
  public void constructor() { 
    assertNotNull(new TSMeta());
  }
  
  @Test
  public void tsuid() {
    meta.setTSUID("ABCD");
    assertEquals(meta.getTSUID(), "ABCD");
  }
  
  @Test
  public void metricNull() {
    assertNull(meta.getMetric());
  }
  
  @Test
  public void metric() {
    UIDMeta metric = new UIDMeta();
    metric.setUID("AB");
    meta.setMetric(metric);
    assertNotNull(meta.getMetric());
  }
  
  @Test
  public void tagsNull() {
    assertNull(meta.getTags());
  }
  
  @Test
  public void tags() {
    meta.setTags(new ArrayList<UIDMeta>());
    assertNotNull(meta.getTags());
  }
  
  @Test
  public void displayName() {
    meta.setDisplayName("Display");
    assertEquals(meta.getDisplayName(), "Display");
  }
  
  @Test
  public void description() {
    meta.setDescription("Description");
    assertEquals(meta.getDescription(), "Description");
  }
  
  @Test
  public void notes() {
    meta.setNotes("Notes");
    assertEquals(meta.getNotes(), "Notes");
  }
  
  @Test
  public void created() {
    meta.setCreated(1328140800L);
    assertEquals(meta.getCreated(), 1328140800L);
  }
  
  @Test
  public void customNull() {
    assertNull(meta.getCustom());
  }
  
  @Test
  public void custom() {
    HashMap<String, String> custom_tags = new HashMap<String, String>();
    custom_tags.put("key", "MyVal");
    meta.setCustom(custom_tags);
    assertNotNull(meta.getCustom());
    assertEquals(meta.getCustom().get("key"), "MyVal");
  }
  
  @Test
  public void units() {
    meta.setUnits("%");
    assertEquals(meta.getUnits(), "%");
  }
  
  @Test
  public void dataType() {
    meta.setDataType("counter");
    assertEquals(meta.getDataType(), "counter");
  }
  
  @Test
  public void max() {
    meta.setMax(42.5);
    assertEquals(meta.getMax(), 42.5, 0.000001);
  }
  
  @Test
  public void min() {
    meta.setMin(142.5);
    assertEquals(meta.getMin(), 142.5, 0.000001);
  }
  
  @Test
  public void lastReceived() {
    meta.setLastReceived(1328140801L);
    assertEquals(meta.getLastReceived(), 1328140801L);
  }
}

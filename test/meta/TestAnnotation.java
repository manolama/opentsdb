package net.opentsdb.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;

import org.junit.Test;

public final class TestAnnotation {
  private final Annotation note = new Annotation();
  
  @Test
  public void constructor() {
    assertNotNull(new Annotation());
  }
  
  @Test
  public void tsuid() {
    note.setTSUID("ABCD");
    assertEquals(note.getTSUID(), "ABCD");
  }
  
  @Test
  public void starttime() {
    note.setStartTime(1328140800L);
    assertEquals(note.getStartTime(), 1328140800L);
  }
  
  @Test
  public void endtime() {
    note.setEndTime(1328140801L);
    assertEquals(note.getEndTime(), 1328140801L);
  }
  
  @Test
  public void description() {
    note.setDescription("MyDescription");
    assertEquals(note.getDescription(), "MyDescription");
  }
  
  @Test
  public void notes() {
    note.setNotes("Notes");
    assertEquals(note.getNotes(), "Notes");
  }
  
  @Test
  public void customNull() {
    assertNull(note.getNotes());
  }
  
  @Test
  public void custom() {
    HashMap<String, String> custom_tags = new HashMap<String, String>();
    custom_tags.put("key", "MyVal");
    note.setCustom(custom_tags);
    assertNotNull(note.getCustom());
    assertEquals(note.getCustom().get("key"), "MyVal");
  }
}

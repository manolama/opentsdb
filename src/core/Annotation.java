package net.opentsdb.core;

import java.util.HashMap;

public class Annotation {
  private String tsuid;
  private long start_time;
  private long end_time;
  private String description;
  private String notes;
  private HashMap<String, String> custom;
  
  
  public String getTsuid() {
    return tsuid;
  }
  public void setTsuid(String tsuid) {
    this.tsuid = tsuid;
  }
  public long getStart_time() {
    return start_time;
  }
  public void setStart_time(long start_time) {
    this.start_time = start_time;
  }
  public long getEnd_time() {
    return end_time;
  }
  public void setEnd_time(long end_time) {
    this.end_time = end_time;
  }
  public String getDescription() {
    return description;
  }
  public void setDescription(String description) {
    this.description = description;
  }
  public String getNotes() {
    return notes;
  }
  public void setNotes(String notes) {
    this.notes = notes;
  }
  public HashMap<String, String> getCustom() {
    return custom;
  }
  public void setCustom(HashMap<String, String> custom) {
    this.custom = custom;
  }
}

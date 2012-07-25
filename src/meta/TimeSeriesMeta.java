package net.opentsdb.meta;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.core.JSON;
import net.opentsdb.uid.UniqueId;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Metadata pertaining to a specific time-series of metric values. This encompasses
 * the specifics for a time series as well as the inherited Metric and TagK meta data
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
final public class TimeSeriesMeta {

  // stored values
  private String uid = "";
  private int retention = 0;
  private int max = 0;
  private int min = 0;
  private int interval = 0;
  private int first_received = 0;
  private int last_received = 0;
  private String notes = "";
  private String units = "";
  private int type = 0;         // type of metric, e.g. counter/rate/something else
  private Map<String, String> custom = new HashMap<String, String>();   
  
  // change variables
  private Boolean c_retention = false;
  private Boolean c_max = false;
  private Boolean c_min = false;
  private Boolean c_notes = false;
  private Boolean c_units = false;
  private Boolean c_custom = false;
  private Boolean c_type = false;
  
  // API only
  private GeneralMeta metric = null;
  // <tag name, meta>
  private Map<String, GeneralMeta> tags = null;
 
  public TimeSeriesMeta(){
  }
  
  public TimeSeriesMeta(byte[] id){
    this.uid = UniqueId.IDtoString(id);
  }
  
  // returns the entire object as a JSON string
  @JsonIgnore  
  public String getJSON(){
    JSON json = new JSON(this);
    return json.getJsonString();
  }
  
  // returns just the data we should put in storage
  @JsonIgnore  
  public String getStorageJSON(){
    this.metric = null;
    this.tags = null;
    JSON json = new JSON(this);
    return json.getJsonString();    
  }
  
  //copies changed variables from the local object to the incoming object
  // use:
  // stored_data = user_data.CopyChanges(stored_data);
  // write stored_data;
  public TimeSeriesMeta CopyChanges(TimeSeriesMeta m){
    if (this.c_retention)
      m.retention = this.retention;
    if (this.c_max)
      m.max = this.max;
    if (this.c_min)
      m.min = this.min;
    if (this.c_notes)
      m.notes = this.notes;
    if (this.c_units)
      m.units = this.units;
    if (this.c_type)
      m.type = this.type;
    if (this.c_custom)
      m.custom = this.custom;
    return m;
  }
  
  // **** GETTERS AND SETTERS ****
  public String getUID(){
    return this.uid;
  }
  
  public int getRetention(){
    return this.retention;
  }
  
  public int getMax(){
    return this.max;
  }
  
  public int getMin(){
    return this.min;
  }
  
  public int getInterval(){
    return this.interval;
  }
  
  public int getFirst_Received(){
    return this.first_received;
  }
  
  public int getLast_Received(){
    return this.last_received;
  }
  
  public String getNotes(){
    return this.notes;
  }
  
  public String getUnits(){
    return this.units;
  }
  
  public int getType(){
    return this.type;
  }
  
  public Map<String, String> getCustom(){
    return this.custom;
  }
  
  public GeneralMeta getMetric(){
    return this.metric;
  }
  
  public Map<String, GeneralMeta> getTags(){
    return this.tags;
  }
  
  public void setUID(final String u){
    this.uid = u;
  }
  
  public void setRetention(final int r){
    this.retention = r;
    this.c_retention = true;
  }
  
  public void setMax(final int m){
    this.max = m;
    this.c_max = true;
  }
  
  public void setMin(final int m){
    this.min = m;
    this.c_min = true;
  }
  
  public void setInterval(final int i){
    this.interval = i;
  }
  
  public void setFirst_Received(final int f){
    this.first_received = f;
  }
  
  public void setLast_Received(final int l){
    this.last_received = l;
  }
  
  public void setNotes(final String n){
    this.notes = n;
    this.c_notes = true;
  }
  
  public void setUnits(final String u){
    this.units = u;
    this.c_units = true;
  }
  
  public void setType(final int t){
    this.type = t;
    this.c_type = true;
  }
  
  public void setCustom(final Map<String, String> c){
    this.custom = c;
    this.c_custom = true;
  }
  
  public void setMetric(final GeneralMeta m){
    this.metric = m;
  }
  
  public void setTags(final Map<String, GeneralMeta> t){
    this.tags = t;
  }
}

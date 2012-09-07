package net.opentsdb.meta;

import java.util.Map;

import net.opentsdb.core.JSON;
import net.opentsdb.uid.UniqueId;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parent class containing common metadata fields and methods. Specific metadata
 * types, such as "metrics" or "tagk" should override with their own fields 
 * 
 * NOTE: Only fields that are passed in will be modified in Hbase. e.g. if you pass in
 * display_name, only display_name will be edited and the remaining fields will stay the same.
 * 
 * WARN: However ALL custom fields MUST be included in writes as they will be overwritten on
 * each write if anything changes
 */
public class GeneralMeta {
  public enum Meta_Type { INVALID, METRICS, TAGK, TAGV, TIMESERIES };
  
  protected static final Logger LOG = LoggerFactory.getLogger(GeneralMeta.class);
  
  protected String uid = "";
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  protected Meta_Type type = Meta_Type.INVALID;
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  protected String name = "";
  protected String display_name = "";
  protected String description = "";
  protected String notes = "";
  protected long created = 0; 
  protected Map<String, String> custom = null;

  // change flags used to determine what the user wants to modify
  protected Boolean c_display_name = false;
  protected Boolean c_description = false;
  protected Boolean c_notes = false;
  protected Boolean c_custom = false;

  public GeneralMeta(){
  }
  
  public GeneralMeta(byte[] id){
    this.uid = UniqueId.IDtoString(id);
  }
  
  public GeneralMeta(byte[] id, Meta_Type type){
    this.uid = UniqueId.IDtoString(id);
    this.type = type;
  }
  
  // returns the contents as a JSON string
  @JsonIgnore
  public String getJSON(){
    JSON json = new JSON(this);
    return json.getJsonString();
  }
  
  // copies changed variables from the local object to the incoming object
  // use:
  // stored_data = user_data.CopyChanges(stored_data);
  // write stored_data;
  public GeneralMeta CopyChanges(GeneralMeta m){
    if (this.c_display_name)
      m.display_name = this.display_name;
    if (this.c_description)
      m.description = this.description;
    if (this.c_notes)
      m.notes = this.notes;
    if (this.c_custom)
      m.custom = this.custom;
    return m;
  }
  
  // **** GETTERS AND SETTERS ****
  public String getUID(){
    return this.uid;
  }

  public Meta_Type getType(){
    return this.type;
  }
  
  public String getName(){
    return this.name;
  }
  
  public String getDisplay_name(){
    return this.display_name;
  }
  
  public String getDescription(){
    return this.description;
  }
  
  public String getNotes(){
    return this.notes;
  }
  
  public long getCreated(){
    return this.created;
  }
  
  public Map<String, String> getCustom(){
    return this.custom;
  }
  
  public void setUID(final String u){
    this.uid = u;
  }
  
  public void setType(Meta_Type t){
    this.type = t;
  }
  
  public void setName(final String n){
    this.name = n;
  }
  
  public void setDisplay_name(final String d){
    this.display_name = d;
    this.c_display_name = true;
  }
  
  public void setDescription(final String d){
    this.description = d;
    this.c_description = true;
  }
  
  public void setNotes(final String n){
    this.notes = n;
    this.c_notes = true;
  }
  
  public void setCreated(final long c){
    this.created = c;
  }
  
  public void setCustom(final Map<String, String> c){
    this.custom = c;
    this.c_custom = true;
  }
}

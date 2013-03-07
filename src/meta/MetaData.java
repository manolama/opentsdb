package net.opentsdb.meta;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import net.opentsdb.uid.UniqueId;

public abstract class MetaData {

  protected String uid = "";
  protected long created = 0;
  protected String notes = "";
  protected Map<String, String> custom = null;
  
  
  // change variables
  protected boolean c_notes = false;
  protected boolean c_custom = false;
  
  public MetaData(){}
  
  public MetaData(final byte[] uid){
    this.uid = UniqueId.IDtoString(uid);
  }
  
  public abstract String getJSON() throws JsonGenerationException, IOException;
  
  public abstract byte[] getJSONBytes() throws JsonGenerationException, IOException;
  
  public abstract MetaData copyChanges(final MetaData metadata);
  
  public abstract void copy(final MetaData metadata);
  
  public abstract void parseJSON(final String json) 
    throws JsonParseException, JsonMappingException, IOException;
  
  // **** GETTERS AND SETTERS ****
  public String getUID(){
    return this.uid;
  }
  
  public long getCreated(){
    return this.created;
  }
  
  public String getNotes(){
    return this.notes;
  }
  
  public Map<String, String> getCustom(){
    return this.custom;
  }
  
  public void setUID(final String u){
    this.uid = u;
  }
  
  public void setCreated(final long c){
    this.created = c;
  }
  
  public void setNotes(final String n){
    this.notes = n;
    this.c_notes = true;
  }
  
  public void setCustom(final Map<String, String> c){
    this.custom = c;
    this.c_custom = true;
  }
}

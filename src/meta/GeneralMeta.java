package net.opentsdb.meta;

import java.util.Map;

import net.opentsdb.core.JSON;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
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
public class GeneralMeta extends MetaData {
  public enum Meta_Type { INVALID, METRICS, TAGK, TAGV, TIMESERIES };
  
  protected static final Logger LOG = LoggerFactory.getLogger(GeneralMeta.class);
  
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_DEFAULT)
  protected Meta_Type type = Meta_Type.INVALID;
  protected String name = "";
  protected String display_name = "";
  protected String description = "";

  // change flags used to determine what the user wants to modify
  protected Boolean c_display_name = false;
  protected Boolean c_description = false;

  public GeneralMeta(){
    super();
  }
  
  public GeneralMeta(byte[] id){
    super(id);
  }
  
  public GeneralMeta(byte[] id, Meta_Type type){
    super(id);
    this.type = type;
  }
  
  public boolean equals(Object meta){
    if (meta == null)
      return false;
    try{
      GeneralMeta m = (GeneralMeta)meta;
      if (this.uid != m.uid)
        return false;
      if (this.created != m.created)
        return false;
      if (this.notes != m.notes)
        return false;
      if (this.custom != m.custom)
        return false;
      if (this.type != m.type)
        return false;
      if (this.name != m.name)
        return false;
      if (this.display_name != m.display_name)
        return false;
      if (this.description != m.description)
        return false;
      
      return true;
    }catch (Exception e){
      return false;
    }
  }
  
  // returns the contents as a JSON string
  @JsonIgnore
  public String getJSON(){
    JSON codec = new JSON(this);
    return codec.getJsonString();
  }
  
  @JsonIgnore
  public byte[] getJSONBytes(){
    JSON codec = new JSON(this);
    return codec.getJsonBytes();
  }
  
  // copies changed variables from the local object to the incoming object
  // use:
  // stored_data = user_data.CopyChanges(stored_data);
  // write stored_data;
  public MetaData copyChanges(MetaData metadata){
    try{
      GeneralMeta m = (GeneralMeta)metadata;
      if (m.name.compareTo(this.name) != 0)
        if (m.name.isEmpty())
          m.name = this.name;
      if (this.c_display_name)
        m.display_name = this.display_name;
      if (this.c_description)
        m.description = this.description;
      if (this.c_notes)
        m.notes = this.notes;
      if (this.c_custom)
        m.custom = this.custom;
      return m;
    }catch (Exception e){
      LOG.error("Unable to cast metadata to proper type");
    }
    return null;
  }
  
  public void copy(final MetaData metadata){
    try{
      GeneralMeta m = (GeneralMeta)metadata;
      this.uid = m.uid;
      this.created = m.created;
      this.notes = m.notes;
      this.custom = m.custom;
      this.type = m.type;
      this.name = m.name;
      this.display_name = m.display_name;
      this.description = m.description;
    }catch(Exception e){
      LOG.warn("Invalid cast for General Metadata");
    }
  }
  
  public boolean parseJSON(final String json){
    try{
      JSON codec = new JSON(this);
      if (!codec.parseObject(json)){
        LOG.warn("Unable to parse JSON");
        return false;
      }
      
      GeneralMeta meta = (GeneralMeta)codec.getObject();
      if (meta == null){
        LOG.error("Error parsing JSON");
        return false;
      }
      
      this.copy(meta);
      return true;
    }catch (Exception e){
      e.printStackTrace();
    }
    return false;
  }
  
  public final boolean appendFields(Document doc, StringBuilder flatten){
    //doc.add(new Field("uid", this.uid, Field.Store.NO, Field.Index.NOT_ANALYZED));
    //doc.add(new Field("type", this.type.toString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
    doc.add(new Field("name", this.name, Field.Store.NO, Field.Index.NOT_ANALYZED));
    flatten.append(this.name + " ");
    doc.add(new Field("display_name", this.display_name, Field.Store.NO, Field.Index.ANALYZED));
    flatten.append(this.display_name + " ");
    doc.add(new Field("description", this.description, Field.Store.NO, Field.Index.ANALYZED));
    flatten.append(this.description + " ");
    doc.add(new Field("notes", this.notes, Field.Store.NO, Field.Index.ANALYZED));
    flatten.append(this.notes + " ");
    doc.add(new NumericField("created").setLongValue(this.created));    
    
    if (this.custom != null){
      for (Map.Entry<String, String> entry : this.custom.entrySet()){
        doc.add(new Field(entry.getKey(), entry.getValue(), Field.Store.NO, Field.Index.ANALYZED));
        flatten.append(entry.getKey() + " ");
        flatten.append(entry.getValue()+ " ");
      }
    }
    return true;
  }
  
  // **** GETTERS AND SETTERS ****
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


}

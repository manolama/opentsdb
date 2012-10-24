package net.opentsdb.meta;

import java.util.ArrayList;
import java.util.Map;

import net.opentsdb.core.JSON;
import net.opentsdb.meta.GeneralMeta.Meta_Type;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata pertaining to a specific time-series of metric values. This encompasses
 * the specifics for a time series as well as the inherited Metric and TagK meta data
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final public class TimeSeriesMeta extends MetaData {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesMeta.class);
  
  // stored values
  private int retention = 0;
  private double max = 0;
  private double min = 0;
  private double interval = 0;
  private long last_received = 0;
  private String units = "";
  private int data_type = 0;         // type of metric, e.g. counter/rate/something else  
  
  // change variables
  private boolean c_retention = false;
  private boolean c_max = false;
  private boolean c_min = false;
  private boolean c_units = false;
  private boolean c_type = false;
  
  // API only
  private GeneralMeta metric = null;
  // <tag name, meta>
  @JsonIgnore
  private Map<GeneralMeta, GeneralMeta> tag_meta = null;
  
  @JsonIgnore
  private ArrayList<GeneralMeta> tags = null;
 
  public TimeSeriesMeta(){
    super();
  }
  
  public TimeSeriesMeta(byte[] id){
    super(id);
  }
  
  public boolean equals(Object meta){
    if (meta == null)
      return false;
    try{
      TimeSeriesMeta m = (TimeSeriesMeta)meta;
      if (this.uid != m.uid)
        return false;
      if (this.created != m.created)
        return false;
      if (this.notes != m.notes)
        return false;
      if (this.custom != m.custom)
        return false;
      if (this.retention != m.retention)
        return false;
      if (this.max != m.max)
        return false;
      if (this.min != m.min)
        return false;
      if (this.interval != m.interval)
        return false;
      if (this.last_received != m.last_received)
        return false;
      if (this.units != m.units)
        return false;
      if (this.data_type != m.data_type)
        return false;
      
      return true;
    }catch (Exception e){
      return false;
    }
  }
  
  // returns the entire object as a JSON string
  @JsonIgnore  
  public String getJSON(){
    if (this.tag_meta != null){
      this.tags = new ArrayList<GeneralMeta>();
      for (Map.Entry<GeneralMeta, GeneralMeta> entry : this.tag_meta.entrySet()){
        this.tags.add(entry.getKey());
        this.tags.add(entry.getValue());
      }
    }
    
    JSON codec = new JSON(this);
    return codec.getJsonString();
  }
  
  @JsonIgnore
  public byte[] getJSONBytes(){
    if (this.tag_meta != null){
      this.tags = new ArrayList<GeneralMeta>();
      for (Map.Entry<GeneralMeta, GeneralMeta> entry : this.tag_meta.entrySet()){
        this.tags.add(entry.getKey());
        this.tags.add(entry.getValue());
      }
    }
    
    JSON codec = new JSON(this);
    return codec.getJsonBytes();
  }
  
  // returns just the data we should put in storage
  @JsonIgnore  
  public String getStorageJSON(){
    this.metric = null;
    this.tag_meta = null;
    JSON json = new JSON(this);
    return json.getJsonString();    
  }
  
  public boolean parseJSON(final String json){
    try{
      JSON codec = new JSON(this);
      if (!codec.parseObject(json)){
        LOG.warn("Unable to parse JSON");
        return false;
      }
      
      TimeSeriesMeta meta = (TimeSeriesMeta)codec.getObject();
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
  
  //copies changed variables from the local object to the incoming object
  // use:
  // stored_data = user_data.CopyChanges(stored_data);
  // write stored_data;
  public MetaData copyChanges(MetaData metadata){
    try{
      TimeSeriesMeta m = (TimeSeriesMeta)metadata;
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
        m.data_type = this.data_type;
      if (this.c_custom)
        m.custom = this.custom;
      if (this.last_received > m.last_received)
        m.last_received = this.last_received;
      return m;
    }catch (Exception e){
      LOG.error("Unable to cast metadata");
      return null;
    }
  }
  
  public void copy(MetaData metadata){
    try{
      TimeSeriesMeta m = (TimeSeriesMeta)metadata;
      this.uid = m.uid;
      if (this.created >= m.created)
        this.created = m.created;
      this.notes = m.notes;
      this.custom = m.custom;
      this.retention = m.retention;
      this.max = m.max;
      this.min = m.min;
      this.interval = m.interval;
      this.last_received = m.last_received;
      this.units = m.units;
      this.data_type = m.data_type;
    }catch(Exception e){
      LOG.warn("Invalid cast for TimeSeriesMeta Metadata");
    }
  }
  
  public final Document buildLuceneDoc(){
    if (this.uid == null || this.uid.length() < 1)
      return null;
    if (this.tag_meta == null || this.tag_meta.size() < 1){
      LOG.warn(String.format("Missing tag meta for TSUID [%s]", uid));
      return null;
    }
    if (this.metric == null){
      LOG.warn(String.format("Missing metric meta for TSUID [%s]", uid));
      return null;
    }
    
    StringBuilder flatten = new StringBuilder();

    // build the document
    Document doc = new Document();
    doc.add(new Field("tsuid", this.uid, Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("metric", this.metric.getName(), Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("metric_uid", this.uid.substring(0, 6), Field.Store.NO, Field.Index.NOT_ANALYZED));
    doc.add(new NumericField("retention").setIntValue(this.retention));
    doc.add(new NumericField("max").setDoubleValue(this.max));
    doc.add(new NumericField("min").setDoubleValue(this.min));
    doc.add(new NumericField("interval").setDoubleValue(this.interval));
    doc.add(new NumericField("created").setLongValue(this.created));
    doc.add(new NumericField("last_received").setLongValue(this.last_received));
    doc.add(new Field("notes", this.notes, Field.Store.NO, Field.Index.ANALYZED));
    flatten.append(this.notes + " ");
    doc.add(new Field("units", this.units, Field.Store.NO, Field.Index.ANALYZED));
    flatten.append(this.units + " ");
    if (this.custom != null){
      for (Map.Entry<String, String> entry : this.custom.entrySet()){
        doc.add(new Field(entry.getKey(), entry.getValue(), Field.Store.NO, Field.Index.ANALYZED));
        flatten.append(entry.getKey() + " ");
        flatten.append(entry.getValue()+ " ");
      }
    }
    
    // add the metric metadata
    this.metric.appendFields(doc, flatten);
    
    for (Map.Entry<GeneralMeta, GeneralMeta> entry : this.tag_meta.entrySet()){
      entry.getKey().appendFields(doc, flatten);
      entry.getValue().appendFields(doc, flatten);
      
      doc.add(new Field("tagk_uid", entry.getKey().getUID(), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(new Field("tagv_uid", entry.getValue().getUID(), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(new Field("tag_pairs", entry.getKey().getUID() + entry.getValue().getUID(), Field.Store.NO, Field.Index.NOT_ANALYZED));
      doc.add(new Field("tags", entry.getKey().getName() + "=" + entry.getValue().getName(), Field.Store.YES, Field.Index.NOT_ANALYZED));
      
      // put tagk/v pair
      doc.add(new Field(entry.getKey().name, entry.getValue().name, Field.Store.NO, Field.Index.NOT_ANALYZED));
    }
    
    // flatten all text 
    doc.add(new Field("content", flatten.toString(), Field.Store.NO, Field.Index.ANALYZED));
    
    return doc;
  }
  
  // **** GETTERS AND SETTERS ****
  public int getRetention(){
    return this.retention;
  }
  
  public double getMax(){
    return this.max;
  }
  
  public double getMin(){
    return this.min;
  }
  
  public double getInterval(){
    return this.interval;
  }

  public String getUnits(){
    return this.units;
  }
  
  public int getData_type(){
    return this.data_type;
  }

  public GeneralMeta getMetric(){
    return this.metric;
  }
  
  @JsonIgnore
  public Map<GeneralMeta, GeneralMeta> getTagsMeta(){
    return this.tag_meta;
  }
  
  @JsonProperty("tags")
  public ArrayList<GeneralMeta> getTags(){
    return this.tags;
  }
  
  public void setRetention(final int r){
    this.retention = r;
    this.c_retention = true;
  }
  
  public void setMax(final double m){
    this.max = m;
    this.c_max = true;
  }
  
  public void setMin(final double m){
    this.min = m;
    this.c_min = true;
  }
  
  public void setInterval(final int i){
    this.interval = i;
  }

  public void setLast_Received(final long l){
    this.last_received = l;
  }

  public void setUnits(final String u){
    this.units = u;
    this.c_units = true;
  }
  
  public void setData_type(final int t){
    this.data_type = t;
    this.c_type = true;
  }

  public void setMetric(final GeneralMeta m){
    this.metric = m;
  }
  
  @JsonIgnore
  public void setTagsMeta(final Map<GeneralMeta, GeneralMeta> t){
    this.tag_meta = t;
  }

  public long getLastReceived() {
    return last_received;
  }

  public void setLastReceived(long last_received) {
    this.last_received = last_received;
  }
}

package net.opentsdb.core;

import java.util.HashMap;
import java.util.Map;

import net.opentsdb.uid.UniqueId;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.hbase.async.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Annotation {
  private static final Logger LOG = LoggerFactory.getLogger(Annotation.class);
  
  private String tsuid;
  private long start_time;
  private long end_time;
  private String description;
  private String notes;
  private HashMap<String, String> custom;
  
  public boolean store(final TSDB tsdb){
    if (this.start_time < 1){
      LOG.error("Invalid start time");
      return false;
    }
    if (this.tsuid == null || this.tsuid.length() < 1){
      LOG.error("Missing TSUID");
      return false;
    }
    
    try{
      final byte[] row = IncomingDataPoints.getNormalizedRow(tsuid, start_time);
      
      final JSON codec = new JSON(this);
      
      final long base_time = IncomingDataPoints.normalizeTimestamp(start_time);
      final short qualifier = (short) ((start_time - base_time) << Const.FLAG_BITS);
      
      tsdb.data_storage.putWithRetry(row, TsdbConfig.ANNOTATE_FAMILY, 
          Bytes.fromShort(qualifier), codec.getJsonBytes(), 0).joinUninterruptibly();
      
      return true;
    }catch (Exception e){
      e.printStackTrace();
      return false;
    }
  }
  
  public boolean delete(final TSDB tsdb){
    if (this.start_time < 1){
      LOG.error("Invalid start time");
      return false;
    }
    if (this.tsuid == null || this.tsuid.length() < 1){
      LOG.error("Missing TSUID");
      return false;
    }
    
    try{
      final byte[] row = IncomingDataPoints.getNormalizedRow(tsuid, start_time);
      final long base_time = IncomingDataPoints.normalizeTimestamp(start_time);
      final short qualifier = (short) ((start_time - base_time) << Const.FLAG_BITS);
      
      tsdb.data_storage.deleteValue(row, TsdbConfig.ANNOTATE_FAMILY, Bytes.fromShort(qualifier)).joinUninterruptibly();
      return true;
    } catch (Exception e){
      e.printStackTrace();
      return false;
    }
  }
  
  public static Annotation getFromStorage(final TSDB tsdb, final String tsuid, final long timestamp){
    try{
      final byte[] row = IncomingDataPoints.getNormalizedRow(tsuid, timestamp);
      final long base_time = IncomingDataPoints.normalizeTimestamp(timestamp);
      final short qualifier = (short) ((timestamp - base_time) << Const.FLAG_BITS);
      
      final byte[] data = tsdb.data_storage.getValue(row, TsdbConfig.ANNOTATE_FAMILY, Bytes.fromShort(qualifier));
      if (data == null)
        return null;
      
      final JSON codec = new JSON(new Annotation());
      if (!codec.parseObject(data)){
        LOG.warn(String.format("Unable to parse annotation from row [%s]", UniqueId.IDtoString(row)));
        return null;
      }
      
      return (Annotation)codec.getObject();
    }catch (Exception e){
      e.printStackTrace();
      return null;
    }
  }
  
  public final Document buildLuceneDoc(){
    if (this.tsuid == null || this.tsuid.length() < 1)
      return null;
    if (this.start_time < 1){
      LOG.warn(String.format("Missing start time for annotation on TSUID [%s]", tsuid));
      return null;
    }
    
    StringBuilder flatten = new StringBuilder();

    // build the document
    Document doc = new Document();
    doc.add(new Field("uid", this.getSearchUID(), Field.Store.NO, Field.Index.NOT_ANALYZED));
    doc.add(new Field("tsuid", this.tsuid.toLowerCase(), Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("description", this.description, Field.Store.YES, Field.Index.ANALYZED));
    flatten.append(this.description).append(" ");
    doc.add(new Field("notes", this.notes, Field.Store.YES, Field.Index.ANALYZED));
    flatten.append(this.notes).append(" ");
    doc.add(new NumericField("start_time").setLongValue(this.start_time));
    doc.add(new NumericField("end_time").setLongValue(this.end_time));
    if (this.custom != null){
      for (Map.Entry<String, String> entry : this.custom.entrySet()){
        doc.add(new Field(entry.getKey(), entry.getValue(), Field.Store.YES, Field.Index.ANALYZED));
        flatten.append(entry.getKey() + " ");
        flatten.append(entry.getValue()+ " ");
      }
    }

    // flatten all text 
    doc.add(new Field("content", flatten.toString(), Field.Store.NO, Field.Index.ANALYZED));
    
    return doc;
  }
  
  @JsonIgnore
  public String getSearchUID(){
    return this.tsuid.toLowerCase() + Long.toString(this.start_time);
  }
  
// GETTERS AND SETTERS
  
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

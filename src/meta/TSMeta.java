package net.opentsdb.meta;

import java.util.ArrayList;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timeseries Metadata is associated with a particular series of data points
 * and includes user configurable values and some stats calculated by OpenTSDB.
 * Whenever a new timeseries is recorded, an associated TSMeta object will
 * be recorded with only the {@tsuid} field configured.
 * 
 * The metric and tag UIDMeta objects are loaded from their respective locations
 * in the data storage system.
 */
public class TSMeta {
  private static final Logger LOG = LoggerFactory.getLogger(TSMeta.class);
  
  /** Hexadecimal representation of the TSUID this metadata is associated with */
  private String tsuid;
  
  /** The metric associated with this timeseries */
  private UIDMeta metric;
  
  /** A list of tagk/tagv pairs of UIDMetadata associated with this timeseries */
  private ArrayList<UIDMeta> tags;
  
  /** An optional, user supplied descriptive name */
  private String display_name;
  
  /** An optional short description of the timeseries */
  private String description;
  
  /** Optional detailed notes about the timeseries */
  private String notes;
  
  /** A timestamp of when this timeseries was first recorded in seconds */
  private long created;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom;
  
  /** An optional field recording the units of data in this timeseries */
  private String units;
  
  /** An optional field used to record the type of data, e.g. counter, gauge */
  private String data_type;
  
  /** How long to keep raw data in this timeseries */
  private int retention;
  
  /** 
   * A user defined maximum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double max;
  
  /** 
   * A user defined minimum value for this timeseries, can be used to 
   * calculate percentages
   */
  private double min; 
  
  /** A system recorded value of how often this data is recorded */
  private double interval;
  
  /** The last time this data was recorded in seconds */
  private double last_received;
}

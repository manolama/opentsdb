package net.opentsdb.meta;

import java.util.HashMap;

import net.opentsdb.uid.UniqueId.UIDType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UIDMeta objects are associated with the {@link UniqueId} of metrics, tag names
 * or tag values. When a new metric, tagk or tagv is generated, a  UIDMeta object
 * will also be written to storage with only the {@link uid}, {@link type} and
 * {@link name} filled out. Users can then modify mutable fields
 */
public class UIDMeta {
  protected static final Logger LOG = LoggerFactory.getLogger(UIDMeta.class);
  
  /** A hexadecimal representation of the UID this metadata is associated with */
  public String uid;
  
  /** The type of UID this metadata represents */
  public UIDType type;
  
  /** 
   * This is the identical name of what is stored in the UID table
   * It cannot be overridden 
   */
  public String name;
  
  /** 
   * An optional, user supplied name used for display purposes only
   * If this field is empty, the {@link name} field should be used
   */
  public String display_name;
  
  /** A short description of what this object represents */
  public String description;
  
  /** Optional, detailed notes about what the object represents */
  public String notes;
  
  /** A timestamp of when this UID was first recorded by OpenTSDB in seconds */
  public long created;
  
  /** Optional user supplied key/values */
  public HashMap<String, String> custom;
}

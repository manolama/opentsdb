// This file is part of OpenTSDB.
// Copyright (C) 2010, 2011  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.uid;

import java.util.Set;
import java.util.TreeSet;

import net.opentsdb.core.JSON;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps various metric, tagk or tagv to a UID object
 * <p>
 * Maps should be written to the "name" column family in the same row as the UID of the 
 * metric, tagk or tagv it's associated with. While it eats up a little more space in Hbase,
 * the overhead incurred on OpenTSDB is minimal on writes since the maps only need to be
 * updated when a new UID is generated. In such an event, the maps could be update 
 * asynchronously in the background and only 3 to maybe a dozen rows need updating, depending
 * on how many tags are associated with the metric.
 * 
 * The benefit of having these maps is for users to have the ability to quickly see what
 * metrics, tag names and values are associated with a given object. 
 * 
 * Please note that depending on the type of UID object this map is associated with, not all
 * fields will have values, hence their initialization to NULL. 
 * 
 * metric -> tagk, tagv, tags, ts
 * tagk -> metric, tagv, ts
 * tagv -> metric, tagk, ts
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniqueIdMap {
  @JsonIgnore
  private static final Logger LOG = LoggerFactory.getLogger(UniqueIdMap.class);
  
  /** The UID of the object this map is associated with */
  @JsonIgnore
  private String uid;

  /** List of unique tagk/tagv pair UID values */
  private Set<String> tags = null;

  /** Flag to determine if an update needs to be flushed to storage */
  private Boolean has_changed = false;
  
  /**
   * Constructor
   * Empty to make Jackson serialization happy
   */
  public UniqueIdMap(){}
  
  /**
   * Constructor
   * @param uid Sets the UID of this object
   */
  public UniqueIdMap(final String uid){
    this.uid = uid;
  }
  
  /**
   * Creates a copy of the given object
   * @param map The object to copy data from
   */
  public final void Copy(final UniqueIdMap map){
    this.uid = map.uid;
    this.tags = map.tags;
  }
  
  /**
   * Stores a UID in the proper set depending on the type provided
   * Also initializes the set if it hasn't been done so yet.
   * @param uid The UID to store
   * @param type The type of UID being written, "metric", "tagk", "tagv", "tags" or "ts"
   * @return True if the put was successful, false if there was an error
   */
  public final Boolean putMap(final String uid, final String type){
    if (uid.length() < 1){
      LOG.error("Missing UID value");
      return false;
    }
    if (type.length() < 1){
      LOG.error("Missing type value");
      return false;
    }
    
    int size = 0;
    if (type.toLowerCase().compareTo("tags") == 0){
      if (this.tags == null)
        tags = new TreeSet<String>();
      size = tags.size();
      tags.add(uid);
      if (tags.size() != size)
        this.has_changed = true;
      return true;
    }
    
    LOG.error(String.format("Unrecognized type value [%s]", type));
    return false;
  }

  /** 
   * Returns the map as a JSON string for storing in HBase or returning via the API
   * @return A string on success, NULL if there was an error serializing
   */
  @JsonIgnore
  public final String getJSONString(){
    // don't bother if all maps are null
    if (tags == null){
      LOG.error("No data was stored in the map");
      return null;
    }

    // serialize
    JSON json = new JSON(this);
    return json.getJsonString();
  }
  
  /**
   * Attempts to load a JSON string into the local map
   * @param raw The JSON string to parse
   * @return True if the load was successful, False if there was an error parsing
   */
  public final Boolean deserialize(final byte[] raw){
    JSON codec = new JSON(this);
    if (!codec.parseObject(raw)){
      LOG.error(String.format("Unable to parse UID Map [%s]", codec.getError()));
      return false;
    }
    this.Copy((UniqueIdMap)codec.getObject());
    return true;
  }
  
  /**
   * Determines if the provided map is identical to the current map
   * Note that the order of values in the sets don't matter as long as all of the 
   * same values are present.
   * @param map Map to compare against
   * @return True if the maps are identical, false if they differ.
   */
  public final Boolean equals(final UniqueIdMap map){
    if (this.uid != map.uid)
      return false;
    if ((this.tags == null) ^ (map.tags == null))
      return false;
    if (this.tags != null && map.tags != null &&
        this.tags.equals(map.tags))
      return false;
    return true;
  }
  
  /**
   * Merges the values from the provided map to the local copy
   * @param map Map to merge into the local copy
   */
  public final void merge(final UniqueIdMap map){
    if (this.tags == null && map.tags != null)
      this.tags = map.tags;
    else if (this.tags != null && map.tags != null)
      this.tags.addAll(map.tags);
  }
  
  /**
   * Extracts a list of timeseries UIDs related to the current object by
   * searching for tag pairs
   * @param ts_uids A list of all of the Timeseries UIDs in the system
   * @param metric_width The width of the metric tag to match against
   * @return A list of matching Timeseries UIDs, may be NULL if there was an error
   * or it may be empty if no TSUIDs were found to match
   */
  @JsonIgnore
  public final Set<String> getTSUIDs(final Set<String> ts_uids, final short metric_width){
    Set<String> ids = new TreeSet<String>();
    // todo(CL) - there MUST be a better way. This could take ages
    for (String pair : this.tags){
      for (String tsuid : ts_uids){
        // need to start AFTER the metric
        if (tsuid.substring(metric_width*2).contains(pair))
          ids.add(tsuid);
      }
    }
    return ids;
  }

  /**
   * Extracts a list of unique tagk or tagvs from the list of pairs
   * @param type Either "tagk" or "tagv" depending on what you need
   * @param tag_width The width of the tags
   * @return A set of unique tagk or tagvs, or null if there was an error. The set
   * may be empty if the pairs is empty
   */
  @JsonIgnore
  public final Set<String> getTags(final String type, final short tag_width){
    Set<String> ids = new TreeSet<String>();
    if (type != "tagk" && type != "tagv"){
      LOG.error(String.format("Invalid tag type [%s]", type));
      return null;
    }
    final boolean is_tagk = type == "tagk" ? true : false;
    for (String pair : this.tags){
      if (is_tagk)
        ids.add(pair.substring(0, tag_width * 2));
      else
        ids.add(pair.substring(tag_width * 2));
    }
    return ids;
  }
  
  /**
   * Extracts a list of metric UIDs related to the current object by
   * searching for tag pairs in the timestamp uid list
   * @param ts_uids A list of all of the Timeseries UIDs in the system
   * @param metric_width The width of the metric tag to match against
   * @return A list of matching metric UIDs, may be NULL if there was an error
   * or it may be empty if no TSUIDs were found to match
   */
  @JsonIgnore
  public final Set<String> getMetrics(final Set<String> ts_uids, final short metric_width){
    Set<String> ids = new TreeSet<String>();
    // todo(CL) - there MUST be a better way. This could take ages
    for (String pair : this.tags){
      for (String tsuid : ts_uids){
        if (tsuid.contains(pair))
          ids.add(tsuid.substring(0, metric_width * 2));
      }
    }
    return ids;
  }
  
  public static final Set<String> getTSUIDs(final Set<String> tags, final Set<String> ts_uids, 
      final short metric_width){
    Set<String> ids = new TreeSet<String>();
    // todo(CL) - there MUST be a better way. This could take ages
    for (String pair : tags){
      for (String tsuid : ts_uids){
        // need to start AFTER the metric
        if (tsuid.substring(metric_width*2).contains(pair))
          ids.add(tsuid);
      }
    }
    return ids;
  }
  
  // GETTERS AND SETTERS
  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public Set<String> getTags() {
    return tags;
  }

  public void setTags(Set<String> tags) {
    this.tags = tags;
  }

  @JsonIgnore
  public Boolean getHasChanged() {
    return this.has_changed;
  }
  
  @JsonIgnore
  public void setHasChanged(final Boolean changed){
    this.has_changed = changed;
  }
}

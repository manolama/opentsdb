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
  
  /** List of unique metric UID values */
  private Set<String> metric = null;
  
  /** List of unique tagk UID values */
  private Set<String> tagk = null;
  
  /** List of unique tagv UID values */
  private Set<String> tagv = null;
  
  /** List of unique tagk/tagv pair UID values */
  private Set<String> tags = null;
  
  /** List of unique time series UID values */
  private Set<String> ts = null;

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
  public void Copy(final UniqueIdMap map){
    this.uid = map.uid;
    this.tagk = map.tagk;
    this.tagv = map.tagv;
    this.tags = map.tags;
    this.metric = map.metric;
  }
  
  /**
   * Stores a UID in the proper set depending on the type provided
   * Also initializes the set if it hasn't been done so yet.
   * @param uid The UID to store
   * @param type The type of UID being written, "metric", "tagk", "tagv", "tags" or "ts"
   * @return True if the put was successful, false if there was an error
   */
  public Boolean putMap(final String uid, final String type){
    if (uid.length() < 1){
      LOG.error("Missing UID value");
      return false;
    }
    if (type.length() < 1){
      LOG.error("Missing type value");
      return false;
    }
    
    if (type.toLowerCase().compareTo("tagk") == 0){
      if (this.tagk == null)
        tagk = new TreeSet<String>();
      tagk.add(uid);
      return true;
    }else if (type.toLowerCase().compareTo("tagv") == 0){
      if (this.tagv == null)
        tagv = new TreeSet<String>();
      tagv.add(uid);
      return true;
    }else if (type.toLowerCase().compareTo("tags") == 0){
      if (this.tags == null)
        tags = new TreeSet<String>();
      tags.add(uid);
      return true;
    }else if (type.toLowerCase().compareTo("ts") == 0){
      if (this.ts == null)
        ts = new TreeSet<String>();
      ts.add(uid);
      return true;
    }else if (type.toLowerCase().compareTo("metric") == 0){
      if (this.metric == null)
        metric = new TreeSet<String>();
      metric.add(uid);
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
  public String getJSONString(){
    // don't bother if all maps are null
    if (tagk == null && tagv == null && tags == null
        && ts == null && metric == null){
      LOG.error("No data was stored in the map");
      return null;
    }

    // serialize
    JSON json = new JSON(this);
    return json.getJsonString();
  }
  
  /**
   * Attempts to load a JSON string into the local map
   * @param map The JSON string to parse
   * @return True if the load was successful, False if there was an error parsing
   */
  public Boolean deserialize(final String map){
    JSON codec = new JSON(this);
    if (!codec.parseObject(map)){
      LOG.error(String.format("Unable to parse UID Map [%s]", codec.getError()));
      return false;
    }
    this.Copy((UniqueIdMap)codec.getObject());
    return true;
  }
  
  // GETTERS AND SETTERS
  
  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }
  
  public Set<String> getTagk() {
    return tagk;
  }

  public void setTagk(Set<String> tagk) {
    this.tagk = tagk;
  }

  public Set<String> getTagv() {
    return tagv;
  }

  public void setTagv(Set<String> tagv) {
    this.tagv = tagv;
  }

  public Set<String> getTags() {
    return tags;
  }

  public void setTags(Set<String> tags) {
    this.tags = tags;
  }

  public Set<String> getTs() {
    return ts;
  }

  public void setTs(Set<String> ts) {
    this.ts = ts;
  }

  public Set<String> getMetric() {
    return metric;
  }

  public void setMetric(Set<String> metric) {
    this.metric = metric;
  }
}

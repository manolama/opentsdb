// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tree;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;

/**
 * A leaf in a tree
 * @since 2.0
 */
public class Leaf implements Comparable<Leaf> {
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** ASCII Leaf prefix */
  private static final byte[] LEAF_PREFIX = "leaf:".getBytes(CHARSET);
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  
  /** The metric associated with this TSUID */
  private String metric = "";
  
  /** The tags associated with this TSUID for API response purposes */
  private HashMap<String, String> tags = null;
  
  /** Display name for the leaf */
  private String display_name = "";  
  
  /** TSUID the leaf links to */
  private String tsuid = "";

  /**
   * Default empty constructor necessary for des/serialization
   */
  public Leaf() {
    
  }

  public Leaf(final String display_name, final String tsuid) {
    this.display_name = display_name;
    this.tsuid = tsuid;
  }
  
  @Override
  public int hashCode() {
    return display_name.hashCode();
  }
  
  /**
   * Just compares the TSUID
   * @param obj The object to compare this to
   * @return True if the TSUIDs are the same or the incoming object is this one
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    
    final Leaf leaf = (Leaf)obj;
    return tsuid.equals(leaf.tsuid);
  }
  
  /**
   * Sorts on the {@code display_name} alphabetically
   * @param leaf The leaf to compare against
   * @return string comparisson
   */
  @Override
  public int compareTo(Leaf leaf) {
    return display_name.compareToIgnoreCase(leaf.display_name);
  }
  
  /** @return A string describing this object */
  @Override
  public String toString() {
    return "name: " + display_name + " tsuid: " + tsuid;
  }
  
  public void storeLeaf(final TSDB tsdb, final byte[] branch_id) {
    final byte[] qualifier = new byte[LEAF_PREFIX.length + (tsuid.length() / 2)];
    System.arraycopy(LEAF_PREFIX, 0, qualifier, 0, LEAF_PREFIX.length);
    final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, LEAF_PREFIX.length, 
        tsuid_bytes.length);
    
    final PutRequest put = new PutRequest(tsdb.uidTable(), branch_id, 
        NAME_FAMILY, qualifier, toStorageJson());
    tsdb.getClient().compareAndSet(put, new byte[0]);
  }
  
  public static Leaf parseFromStorage(final TSDB tsdb, final KeyValue column, 
      final boolean load_uids) {
    if (column.value() == null) {
      throw new IllegalArgumentException("Leaf column value was null");
    }
    // qualifier has the TSUID in the format  "leaf:<tsuid>"
    // and we should only be here if the qualifier matched on "leaf:"
    final Leaf leaf = JSON.parseToObject(column.value(), Leaf.class);
    leaf.tsuid = UniqueId.uidToString(Arrays.copyOfRange(column.qualifier(), 
        LEAF_PREFIX.length, column.qualifier().length));
    
    if (load_uids) {
      final byte[] metric = UniqueId.stringToUid(
          leaf.tsuid.substring(0, TSDB.metrics_width() * 2));
      leaf.metric = tsdb.getUidName(UniqueIdType.METRIC, metric);
      
      List<byte[]> tags = UniqueId.getTagPairsFromTSUID(leaf.tsuid, 
          TSDB.metrics_width(), TSDB.tagk_width(), TSDB.tagv_width());
      leaf.tags = new HashMap<String, String>();
      int idx = 0;
      String tagk = "";
      for (byte[] uid : tags) {
        if (idx % 2 == 0) {
          tagk = tsdb.getUidName(UniqueIdType.TAGK, uid);
        } else {
          final String tagv = tsdb.getUidName(UniqueIdType.TAGV, uid);
          leaf.tags.put(tagk, tagv);
        }
        idx++;
      }
    }
    return leaf;
  }
  
  public static byte[] LEAF_PREFIX() {
    return LEAF_PREFIX;
  }
  
  private byte[] toStorageJson() {
    final ByteArrayOutputStream output = new ByteArrayOutputStream(
        display_name.length() + 20);
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      json.writeObjectField("displayName", display_name);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  // GETTERS AND SETTERS ----------------------------

  /** @return The metric associated with this TSUID */
  public String getMetric() {
    return metric;
  }
  
  /** @return The tags associated with this TSUID */
  public Map<String, String> getTags() {
    return tags;
  }
  
  /** @return The public name of this leaf */
  public String getDisplayName() {
    return display_name;
  }

  /** @return the tsuid */
  public String getTsuid() {
    return tsuid;
  }

  /** @param metric The metric associated with this TSUID */
  public void setMetric(final String metric) {
    this.metric = metric;
  }
  
  /** @param tags The tags associated with this TSUID */
  public void setTags(final HashMap<String, String> tags) {
    this.tags = tags;
  }
  
  /** @param display_name Public display name for the leaf */
  public void setDisplayName(final String display_name) {
    this.display_name = display_name;
  }

  /** @param tsuid the tsuid to set */
  public void setTsuid(final String tsuid) {
    this.tsuid = tsuid;
  }
  
}

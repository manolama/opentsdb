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

/**
 * A leaf in a tree
 * @since 2.0
 */
public class Leaf implements Comparable<Leaf> {

  /** ID of the parent branch, if 0, this leaf is a root leaf */
  private int parent_id;
  
  /** Depth of the leaf */
  private int depth;
  
  /** Private name for the leaf */
  private String name;
  
  /** Display name for the leaf */
  private String display_name;  
  
  /** TSUID the leaf links to */
  private String tsuid;

  /** @return Returns the hash code of the {@code display_name} value */
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
    return display_name.compareTo(leaf.display_name);
  }
  
  /** @return A string describing this object */
  @Override
  public String toString() {
    return com.google.common.base.Objects.toStringHelper(this)
      .addValue(parent_id)
      .addValue(display_name)
      .addValue(tsuid)
      .toString();
  }
  
  // GETTERS AND SETTERS ----------------------------

  /** @return ID of the parent branch */
  public int getParentId() {
    return parent_id;
  }

  /** @return Depth of this leaf */
  public int getDepth() {
    return depth;
  }

  /** @return The internal name of this leaf */
  public String getName() {
    return name;
  }

  /** @return The public name of this leaf */
  public String getDisplayName() {
    return display_name;
  }

  /** @return the tsuid */
  public String getTsuid() {
    return tsuid;
  }

  /** @param parent_id The ID of the branch that owns this leaf */
  public void setParentId(int parent_id) {
    this.parent_id = parent_id;
  }

  /** @param depth Depth for the leaf */
  public void setDepth(int depth) {
    this.depth = depth;
  }

  /** @param name Internal name of the leaf */
  public void setName(String name) {
    this.name = name;
  }

  /** @param display_name Public display name for the leaf */
  public void setDisplayName(String display_name) {
    this.display_name = display_name;
  }

  /** @param tsuid the tsuid to set */
  public void setTsuid(String tsuid) {
    this.tsuid = tsuid;
  }
  
}

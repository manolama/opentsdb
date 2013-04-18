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
import java.util.TreeSet;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.utils.JSON;

/**
 * 
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class Branch implements Comparable<Branch> {

  /** The tree this branch belongs to */
  private int tree_id;
  
  /** The ID of this branch */
  private int branch_id;
  
  /** ID of the parent of this branch, 0 means this is root */
  private int parent_branch_id;

  /** Depth of the branch */
  private int depth;
  
  /** Private name for the branch */
  private String name;
  
  /** Display name for the branch */
  private String display_name;
  
  /** Used for derialization only */
  private int num_branches;
  
  /** Used for derialization only */
  private int num_leaves;
  
  /** Sorted set of leaves belonging to this branch */
  private TreeSet<Leaf> leaves;
  
  /** Sorted set of child branches */
  private TreeSet<Branch> branches;

  /** @return Returns the {@code branch_id} as the hash code */
  @Override
  public int hashCode() {
    return branch_id;
  }
  
  /**
   * Just compares the branch ID
   * @param obj The object to compare this to
   * @return True if the branch IDs are the same or the incoming object is 
   * this one
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
    
    final Branch branch = (Branch)obj;
    return branch_id == branch.branch_id;
  }
  
  /**
   * Comparator based on the "display_name" to sort branches when displaying
   */
  @Override
  public int compareTo(Branch branch) {
    return this.display_name.compareTo(branch.display_name);
  }
  
  /**
   * Returns a byte array with the branch serialized to JSON since we want
   * some fields to go to storage and others to go to the user
   * @param for_storage Whether or not to serialize for storage only. This will
   * drop some unnecessary fields to save a tiny bit of space
   * @return A byte array for storage or display
   */
  public byte[] toJson(final boolean for_storage) {
    // TODO calculate an initial allocation size
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      if (!for_storage) {
        json.writeNumberField("treeId", tree_id);
      }
      json.writeNumberField("branchId", branch_id);
      json.writeNumberField("parentId", parent_branch_id);
      json.writeNumberField("depth", depth);
      json.writeStringField("displayName", display_name);
      if (!for_storage) {
        json.writeNumberField("numBranches", 
            branches == null ? 0 : branches.size());
        json.writeNumberField("numLeaves", 
            leaves == null ? 0 : leaves.size());
      }
      
      if (branches == null) {
        json.writeNullField("branches");
      } else {
        json.writeArrayFieldStart("branches");
        for (Branch branch : branches) {
          json.writeStartObject();
          json.writeNumberField("branchId", branch.branch_id);
          json.writeStringField("displayName", branch.display_name);
          json.writeNumberField("numBranches", branch.num_branches);
          json.writeNumberField("numLeaves", branch.num_leaves);
          json.writeEndObject();
        }
        json.writeEndArray();
      }
      
      if (leaves == null) {
        json.writeNullField("leaves");
      } else {
        json.writeArrayFieldStart("leaves");
        for (Leaf leaf : leaves) {
          json.writeStartObject();
          json.writeNumberField("depth", leaf.getDepth());
          json.writeStringField("displayName", leaf.getDisplayName());
          json.writeStringField("tsuid", leaf.getTsuid());
          json.writeEndObject();
        }
        json.writeEndArray();
      }
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // GETTERS AND SETTERS ----------------------------
  
  /** @return The ID of the tree this branch belongs to */
  public int getTreeId() {
    return tree_id;
  }

  /** @return The ID of this branch */
  public int getBranchId() {
    return branch_id;
  }

  /** @return The ID of the parent branch */
  public int getParentBranchId() {
    return parent_branch_id;
  }

  /** @return Depth of this branch */
  public int getDepth() {
    return depth;
  }

  /** @return The internal name of this branch */
  public String getName() {
    return name;
  }
  
  /** @return Number of leaves in this branch */
  public int getNumLeaves() {
    return leaves == null ? 0 : leaves.size();
  }

  /** @return Total number of child branches */
  public int getNumBranches() {
    return branches == null ? 0 : branches.size();
  }

  /** @return Name to display to the public */
  public String getDisplayName() {
    return display_name;
  }

  /** @return Ordered set of leaves belonging to this branch */
  public TreeSet<Leaf> getLeaves() {
    return leaves;
  }

  /** @return Ordered set of child branches */
  public TreeSet<Branch> getBranches() {
    return branches;
  }

  /** @param tree_id ID of the tree this branch belongs to */
  public void setTreeId(int tree_id) {
    this.tree_id = tree_id;
  }

  /** @param branch_id ID of the branch */
  public void setBranchId(int branch_id) {
    this.branch_id = branch_id;
  }

  /** @param parent_branch_id ID of the parent branch */
  public void setParentBranchId(int parent_branch_id) {
    this.parent_branch_id = parent_branch_id;
  }

  /** @param depth Depth for the leaf */
  public void setDepth(int depth) {
    this.depth = depth;
  }

  /** @param name Internal name of the leaf */
  public void setName(String name) {
    this.name = name;
  }
  
  /** @param num_branches Set the number of branches, used for deserialization */
  public void setNumBranches(int num_branches) {
    this.num_branches = num_branches;
  }
  
  /** @param num_leaves Set the number of leaves, used for deserialization */
  public void setNumLeaves(int num_leaves) {
    this.num_leaves = num_leaves;
  }
  
  /** @param display_name Public name to display */
  public void setDisplayName(String display_name) {
    this.display_name = display_name;
  }

  /** @param leaves A set of leaves */
  public void setLeaves(TreeSet<Leaf> leaves) {
    this.leaves = leaves;
  }

  /** @param branches A set of branches */
  public void setBranches(TreeSet<Branch> branches) {
    this.branches = branches;
  }  
  
 }

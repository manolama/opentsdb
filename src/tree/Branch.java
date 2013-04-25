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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.bind.DatatypeConverter;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * 
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class Branch implements Comparable<Branch> {
  private static final Logger LOG = LoggerFactory.getLogger(Branch.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** The prefix to use when addressing branch storage rows */
  private static byte TREE_BRANCH_PREFIX = 0x01;
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  
  /** The tree this branch belongs to */
  private int tree_id;
  
  /** The ID of this branch */
  private int branch_id;
  
  /** ID of the parent of this branch, 0 means this is root */
  private int parent_branch_id;

  /** Depth of the branch */
  private int depth;
  
  /** Private name for the branch */
  private String name = "";
  
  /** Display name for the branch */
  private String display_name = "";
  
  /** Used for derialization only */
  private int num_branches;
  
  /** Used for derialization only */
  private int num_leaves;

  /** Sorted set of leaves belonging to this branch */
  //private TreeSet<Leaf> leaves;
  private HashMap<Integer, Leaf> leaves;
  
  /** Sorted set of child branches */
  //private TreeSet<Branch> branches;
  private HashMap<Integer, Branch> branches;

  /**
   * Default empty constructor necessary for de/serialization
   */
  public Branch() {
    
  }
  
  /**
   * Constructor that sets the tree ID
   * @param tree_id ID of the tree this branch is associated with
   */
  public Branch(final Tree tree) {
    tree_id = tree.getTreeId();
  }
  
  /**
   * Constructor that sets the tree ID
   * @param tree_id ID of the tree this branch is associated with
   * @param parent_id ID of the parent this branch belongs to
   */
  public Branch(final int tree_id, final int parent_id) {
    this.tree_id = tree_id;
    this.parent_branch_id = parent_id;
  }
  
  /** @return Returns the {@code branch_id} as the hash code */
  @Override
  public int hashCode() {
    if (name == null || name.isEmpty()) {
      return 0;
    }
    return name.hashCode();
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
    return this.display_name.compareToIgnoreCase(branch.display_name);
  }
  
  /** @return Information about this branch including ID, name and display name */
  @Override
  public String toString() {
    return "ID: [" + hashCode() + "] name: [" + name + 
    "] displayName[" + display_name + "]";
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
      
      json.writeNumberField("treeId", tree_id);
      if (for_storage) {
        json.writeNumberField("branchId", branch_id);
      } else {
        json.writeStringField("branchId", idToString(branch_id));
      }
      if (for_storage) {
        json.writeNumberField("parentId", parent_branch_id);
      } else {
        json.writeStringField("parentId", idToString(parent_branch_id));
      }
      json.writeNumberField("depth", depth);
      json.writeStringField("displayName", display_name);
      if (!for_storage) {
        json.writeNumberField("numBranches", 
            branches == null ? 0 : branches.size());
        json.writeNumberField("numLeaves", 
            leaves == null ? 0 : leaves.size());
      } else {
        json.writeStringField("name", name);
      }
      
      if (branches == null) {
        json.writeNullField("branches");
      } else {
        json.writeArrayFieldStart("branches");
        for (Branch branch : branches.values()) {
          json.writeStartObject();
          if (for_storage) {
            json.writeNumberField("branchId", branch.branch_id);
          } else {
            json.writeStringField("branchId", idToString(branch.branch_id));
          }
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
        for (Leaf leaf : leaves.values()) {
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

  /**
   * Adds a child branch to the local object, merging if the branch already
   * exists. Also checks for leaf collisions.
   * @param branch The branch to add
   * @param tree The tree to report to with collisions
   * @return True if there were changes, false if the child and all of it's 
   * leaves and branches already existed
   * @throws IllegalArgumentException if the incoming branch is null
   */
  public boolean addChild(final Branch branch, final Tree tree) {
    if (branch == null) {
      throw new IllegalArgumentException("Null branches are not allowed");
    }
    if (branches == null) {
      branches = new HashMap<Integer, Branch>();
      branches.put(branch.getBranchId(), branch);
      return true;
    }
    
    final Branch local_branch = branches.get(branch.hashCode());
    if (local_branch == null) {
      branches.put(branch.hashCode(), branch);
      return true;
    }
    
    return local_branch.mergeBranches(branch, tree);
  }
  
  /**
   * Adds a leaf to the local branch, looking for collisions
   * @param leaf The leaf to add
   * @param tree The tree to report to with collisions
   * @return True if there were changes, false if the leaf already 
   * exists or would cause a collision
   * @throws IllegalArgumentException if the incoming leaf is null
   */
  public boolean addLeaf(final Leaf leaf, final Tree tree) {
    if (leaf == null) {
      throw new IllegalArgumentException("Null leaves are not allowed");
    }
    if (leaves == null) {
      leaves = new HashMap<Integer, Leaf>();
      leaves.put(leaf.hashCode(), leaf);
      return true;
    }
    
    if (leaves.containsKey(leaf.hashCode())) {
      // if we try to sync a leaf with the same hash of an existing key
      // but a different TSUID, it's a collision, so mark it
      if (!leaves.get(leaf.hashCode()).getTsuid().equals(leaf.getTsuid())) {
        if (tree != null) {
          tree.addCollision(leaf.getTsuid());
        }
        
        // log at info or lower since it's not a system error, rather it's
        // a user issue with the rules or naming schema
        final Leaf collision = leaves.get(leaf.hashCode());
        LOG.info("Incoming TSUID [" + leaf.getTsuid() + 
            "] collided with existing TSUID [" + collision.getTsuid() + 
            "] on display name [" + collision.getDisplayName() + "]");
      }
      return false;
    } else {
      leaves.put(leaf.hashCode(), leaf);
      return true;
    }
  }
  
  /**
   * Attempts to merge a branch into the local object, merging the child leaves
   * and branches.
   * @param branch The branch to merge into the local branch
   * @param tree The tree to report to with collisions
   * @return True if the merge created changes and the branch should be saved,
   * false if no changes were detected.
   * @throws IllegalArgumentException if the incoming branch ID is not the same
   * as the local ID.
   */
  public boolean mergeBranches(final Branch branch, final Tree tree) {
    if (branch_id != branch.branch_id) {
      throw new IllegalArgumentException(
          "Incoming branch ID does not match local ID");
    }
    
    boolean changed = false;
    // sync branches
    if (branch.branches != null && !branch.branches.isEmpty()) {
      if (branches == null) {
        branches = new HashMap<Integer, Branch>();
        branches.putAll(branch.branches);
        changed = true;
      } else {
        for (Map.Entry<Integer, Branch>  entry : branch.branches.entrySet()) {
          if (!branches.containsKey(entry.getKey())) {
            branches.put(entry.getKey(), entry.getValue());
            changed = true;
          }
        }
      }
    }
    
    // sync leaves
    if (branch.leaves != null && !branch.leaves.isEmpty()) {
      if (leaves == null) {
        leaves = new HashMap<Integer, Leaf>();
        leaves.putAll(branch.leaves);
        changed = true;
      } else {
        for (Map.Entry<Integer, Leaf> entry : branch.leaves.entrySet()) {
          final int key = entry.getKey();
          final Leaf leaf = entry.getValue();
          if (leaves.containsKey(key)) {
            // if we try to sync a leaf with the same hash of an existing key
            // but a different TSUID, it's a collision, so mark it
            if (!leaves.get(key).getTsuid().equals(leaf.getTsuid())) {
              if (tree != null) {
                tree.addCollision(leaf.getTsuid());
              }
              
              // log at info or lower since it's not a system error, rather it's
              // a user issue with the rules or naming schema
              final Leaf collision = leaves.get(key);
              LOG.info("Incoming TSUID [" + leaf.getTsuid() + 
                  "] collided with existing TSUID [" + collision.getTsuid() + 
                  "] on display name [" + collision.getDisplayName() + "]");
            }
          } else {
            leaves.put(entry.getKey(), entry.getValue());
            changed = true;
          }
        }
      }
    }
    
    return changed;
  }
  
  /**
   * Attempts to write the branch to storage with a lock on the row
   * @param tsdb The TSDB to use for access
   * @param lock An optional row lock
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the object could not be serialized
   */
  public void storeBranch(final TSDB tsdb, final RowLock lock) {  
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    
    // row ID = [ prefix, tree_id ] so we just get the tree ID as a single byte
    final byte[] row = { TREE_BRANCH_PREFIX, ((Integer)tree_id).byteValue() };  

    final PutRequest put;
    if (lock == null) {
      put = new PutRequest(tsdb.uidTable(), row, NAME_FAMILY, 
          Bytes.fromInt(branch_id), toJson(true));
    } else {
      put = new PutRequest(tsdb.uidTable(), row, NAME_FAMILY, 
          Bytes.fromInt(branch_id), toJson(true), lock);
    }
    tsdb.hbasePutWithRetry(put, (short)3, (short)800);
  }
  
  /**
   * Attempts to retrieve the branch from storage, optionally with a lock
   * @param tsdb The TSDB to use for access
   * @param tree_id The Tree row to fetch from
   * @param branch_id The ID of the branch to fetch
   * @param lock An optional lock if performing an atomic sync
   * @return The branch from storage
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   */
  public static Branch fetchBranch(final TSDB tsdb, final int tree_id, 
      final int branch_id, final RowLock lock) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    
    // row ID = [ prefix, tree_id ] so we just get the tree ID as a single byte
    final byte[] key = { TREE_BRANCH_PREFIX, ((Integer)tree_id).byteValue() };
    final GetRequest get = new GetRequest(tsdb.uidTable(), key);
    get.family(NAME_FAMILY);
    get.qualifier(Bytes.fromInt(branch_id));
    if (lock != null) {
      get.withRowLock(lock);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return JSON.parseToObject(row.get(0).value(), Branch.class);
    } catch (HBaseException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (JSONException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Converts a branch ID hash to a hex encoded, upper case string with padding
   * @param branch_id The ID to convert
   * @return the branch ID as an 8 character hex string
   */
  public static String idToString(final int branch_id) {
    return DatatypeConverter.printHexBinary(Bytes.fromInt(branch_id));
  }
  
  /**
   * Converts a hex string to an integer branch ID
   * @param branch_id The branch ID to convert
   * @return The branch ID as an integer value
   * @throws NullPointerException if the branch ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   */
  public static int stringToId(final String branch_id) {
    if (branch_id == null || branch_id.isEmpty()) {
      throw new IllegalArgumentException("Branch ID was empty");
    }
    if (branch_id.length() > 8) {
      throw new IllegalArgumentException("Branch ID was too long");
    }
    String id = branch_id;
    while (id.length() < 8) {
      id = "0" + id;
    }
    return Bytes.getInt(DatatypeConverter.parseHexBinary(id));
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
    if (leaves == null) {
      return null;
    }
    return new TreeSet<Leaf>(leaves.values());
  }

  /** @return Ordered set of child branches */
  public TreeSet<Branch> getBranches() {
    if (branches == null) {
      return null;
    }
    return new TreeSet<Branch>(branches.values());
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
  public void setParentId(int parent_branch_id) {
    this.parent_branch_id = parent_branch_id;
  }

  /** @param depth Depth for the leaf */
  public void setDepth(int depth) {
    this.depth = depth;
  }

  /** @param name Internal name of the leaf */
  public void setName(String name) {
    this.name = name;
    this.branch_id = name.hashCode();
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

  /** @param leaves A set of leaves to store */
  public void setLeaves(TreeSet<Leaf> leaves) {
    if (leaves == null) {
      this.leaves = null;
      return;
    }
    
    // blow out the old map
    this.leaves = new HashMap<Integer, Leaf>(leaves.size());
    for (Leaf leaf : leaves) {
      this.leaves.put(leaf.hashCode(), leaf);
    }
  }

  /** @param branches A set of branches */
  public void setBranches(TreeSet<Branch> branches) {
    if (branches == null) {
      this.branches = null;
      return;
    }
    
    // blow out the old map
    this.branches = new HashMap<Integer, Branch>(branches.size());
    for (Branch branch : branches) {
      this.branches.put(branch.getBranchId(), branch);
    }
  }  
  
 }

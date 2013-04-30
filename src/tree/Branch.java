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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.DatatypeConverter;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

/**
 * 
 * IDS = Hex encoded byte arrays composed of the tree ID + hash of each previous
 * branch. Tree ID is encoded on 2 bytes, each hash is then 4 bytes. So the root
 * for tree # 1 is just {@code 0001}. A child branch could be 
 * {@code 00001A3B190C2} and so on
 * 
 * branch object in storage:
 *  
 * 
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class Branch implements Comparable<Branch> {
  private static final Logger LOG = LoggerFactory.getLogger(Branch.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  /** Integer width in bytes */
  private static final short INT_WIDTH = 4;
  /** Name of the branch qualifier ID */
  private static final byte[] BRANCH_QUALIFIER = "branch".getBytes(CHARSET);
  
  /** The tree this branch belongs to */
  private int tree_id;

  /** Display name for the branch */
  private String display_name = "";

  /** Hash map of leaves belonging to this branch */
  private HashMap<Integer, Leaf> leaves;
  
  /** Hash map of child branches */
  private TreeSet<Branch> branches;

  /** The path/name of the branch */
  private TreeMap<Integer, String> path;
  
  /**
   * Default empty constructor necessary for de/serialization
   */
  public Branch() {
    
  }

  /**
   * Constructor that sets the tree ID
   * @param tree_id ID of the tree this branch is associated with
   * @throws IllegalArgumentException if the parent path is null
   */
  public Branch(final int tree_id) {
    this.tree_id = tree_id;
  }
  
  /** @return Returns the {@code display_name}'s hash code */
  @Override
  public int hashCode() {
    if (display_name == null || display_name.isEmpty()) {
      return 0;
    }
    return display_name.hashCode();
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
    return display_name == branch.display_name;
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
    return "ID: [" + hashCode() + "] name: [" + display_name + "]";
  }
  
  /**
   * Adds a child branch to the local object, merging if the branch already
   * exists. Also checks for leaf collisions.
   * @param branch The branch to add
   * @return True if there were changes, false if the child and all of it's 
   * leaves and branches already existed
   * @throws IllegalArgumentException if the incoming branch is null
   */
  public boolean addChild(final Branch branch) {
    if (branch == null) {
      throw new IllegalArgumentException("Null branches are not allowed");
    }
    if (branches == null) {
      branches = new TreeSet<Branch>();
      branches.add(branch);
      return true;
    }
    
    if (branches.contains(branch)) {
      return false;
    }
    branches.add(branch);
    return true;
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
        final Leaf collision = leaves.get(leaf.hashCode());
        if (tree != null) {
          tree.addCollision(leaf.getTsuid(), collision.getTsuid());
        }
        
        // log at info or lower since it's not a system error, rather it's
        // a user issue with the rules or naming schema
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
   * Attempts to write the branch to storage with a lock on the row
   * @param tsdb The TSDB to use for access
   * @param lock An optional row lock
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the object could not be serialized
   */
  public void storeBranch(final TSDB tsdb, final boolean store_leaves) {  
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    
    // compile the row key by making sure the display_name is in the path set
    // row ID = <treeID>[<parent.display_name.hashCode()>...]
    final byte[] row = this.compileBranchId(); 
    
    // compile the object for storage, this will toss exceptions if we are
    // missing anything important
    final byte[] storage_data = toStorageJson();

    final PutRequest put = new PutRequest(tsdb.uidTable(), row, NAME_FAMILY, 
        BRANCH_QUALIFIER, storage_data);
    put.setBufferable(true);
    tsdb.getClient().compareAndSet(put, new byte[0]);
    
    // store leaves
    if (store_leaves && leaves != null && !leaves.isEmpty()) {
      for (Leaf leaf : leaves.values()) {
        leaf.storeLeaf(tsdb, row);
      }
    }
  }
  
  public static Branch fetchBranch(final TSDB tsdb, final byte[] branch_id, 
      final boolean load_tsuids) {   
    final byte[] id = branch_id;
    final Scanner scanner = setupScanner(tsdb, id);
    final Branch branch = new Branch();
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        System.out.println("Rows: " + rows.size());
        for (final ArrayList<KeyValue> row : rows) {
          System.out.println("Columns: " + row.size());
          for (KeyValue column : row) {
            System.out.println("Qual: " + new String(column.qualifier()));
            if (Bytes.equals(BRANCH_QUALIFIER, column.qualifier())) {
              if (Bytes.equals(id, column.key())) {
                // it's *this* branch. We deserialize to a new object and copy
                // since the columns could be in any order and we may get a 
                // leaf before the branch
                final Branch local_branch = JSON.parseToObject(column.value(), 
                    Branch.class);
                branch.path = local_branch.path;
                branch.display_name = local_branch.display_name;
                branch.tree_id = Tree.bytesToId(column.key());
              } else {
                // it's a child branch
                if (branch.branches == null) {
                  branch.branches = new TreeSet<Branch>();
                  final Branch child = JSON.parseToObject(column.value(), 
                      Branch.class);
                  child.tree_id = Tree.bytesToId(column.key());
                  branch.branches.add(child);
                }
              }
            } else if (Bytes.memcmp(Leaf.LEAF_PREFIX(), column.qualifier(), 0, 
                Leaf.LEAF_PREFIX().length) == 0) {
              if (Bytes.equals(id, column.key())) {
                // process a leaf and skip if the UIDs for the TSUID can't be 
                // found
                try {
                  if (branch.leaves == null) {
                    branch.leaves = new HashMap<Integer, Leaf>();
                  }
                  final Leaf leaf = Leaf.parseFromStorage(tsdb, column, 
                      load_tsuids);
                  branch.leaves.put(leaf.hashCode(), leaf); 
                  System.out.println("Adding leaf: " + leaf);
                } catch (NoSuchUniqueId nsu) {
                  LOG.debug("Invalid UID for in branch: " + branch_id, nsu);
                }
              }
            } else {
              System.out.println("Unrecognized column: " + new String(column.qualifier(), CHARSET));
            }
          }
        }
      }
      
      // if nothing was found, we just return null
      if (branch.tree_id < 0 || branch.path == null) {
        return null;
      }
      return branch;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Converts a branch ID hash to a hex encoded, upper case string with padding
   * @param branch_id The ID to convert
   * @return the branch ID as a character hex string
   */
  public static String idToString(final byte[] branch_id) {
    return DatatypeConverter.printHexBinary(branch_id);
  }
  
  /**
   * Converts a hex string to a branch ID byte array (row key)
   * @param branch_id The branch ID to convert
   * @return The branch ID as a byte array
   * @throws NullPointerException if the branch ID was null
   * @throws IllegalArgumentException if the string is not valid hex
   */
  public static byte[] stringToId(final String branch_id) {
    if (branch_id == null || branch_id.isEmpty()) {
      throw new IllegalArgumentException("Branch ID was empty");
    }
    if (branch_id.length() < 4) {
      throw new IllegalArgumentException("Branch ID was too long");
    }
    String id = branch_id;
    if (id.length() % 2 != 0) {
      id = "0" + id;
    }
    return DatatypeConverter.parseHexBinary(id);
  }

  public static byte[] BRANCH_QUALIFIER() {
    return BRANCH_QUALIFIER;
  }
  
  public byte[] compileBranchId() {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    // root branch path may be empty
    if (path == null) {
      throw new IllegalArgumentException("Missing branch path");
    }
    if (display_name == null || display_name.isEmpty()) {
      throw new IllegalArgumentException("Missing display name");
    }
    // first, make sure the display name is at the tip of the tree set
    if (path.isEmpty()) {
      path.put(0, display_name);
    } else if (!path.lastEntry().getValue().equals(display_name)) {
      final int depth = path.lastEntry().getKey() + 1;
      path.put(depth, display_name);
    }
    
    final byte[] branch_id = new byte[Tree.TREE_ID_WIDTH() + 
                                      ((path.size() - 1) * INT_WIDTH)];
    int index = 0;
    final byte[] tree_bytes = Tree.idToBytes(tree_id);
    System.arraycopy(tree_bytes, 0, branch_id, index, tree_bytes.length);
    index += tree_bytes.length;
    
    for (Map.Entry<Integer, String> entry : path.entrySet()) {
      // skip the root, keeps the row keys 4 bytes shorter
      if (entry.getKey() == 0) {
        continue;
      }
      
      final byte[] hash = Bytes.fromInt(entry.getValue().hashCode());
      System.arraycopy(hash, 0, branch_id, index, hash.length);
      index += hash.length;
    }
    
    return branch_id;
  }
  
  public void prependParentPath(final Map<Integer, String> parent_path) {
    if (parent_path == null) {
      throw new IllegalArgumentException("Parent path was null");
    }
    path = new TreeMap<Integer, String>();
    path.putAll(parent_path);
  }
  
  /**
   * Returns serialized data for the branch to put in storage
   * @return A byte array for storage
   */
  private byte[] toStorageJson() {
    // grab some memory to avoid reallocs
    final ByteArrayOutputStream output = new ByteArrayOutputStream(
        (display_name.length() * 2) + (path.size() * 128));
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      json.writeObjectField("path", path);
      json.writeStringField("displayName", display_name);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Scanner setupScanner(final TSDB tsdb, final byte[] branch_id) {
    final byte[] start = branch_id;
    final byte[] end = Arrays.copyOf(branch_id, branch_id.length);
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start);
    
    // increment the tree ID so we scan the whole tree
    byte[] tree_id = new byte[INT_WIDTH];
    for (int i = 0; i < Tree.TREE_ID_WIDTH(); i++) {
      tree_id[i + (INT_WIDTH - Tree.TREE_ID_WIDTH())] = end[i];
    }
    int id = Bytes.getInt(tree_id) + 1;
    tree_id = Bytes.fromInt(id);
    for (int i = 0; i < Tree.TREE_ID_WIDTH(); i++) {
      end[i] = tree_id[i + (INT_WIDTH - Tree.TREE_ID_WIDTH())];
    }
    scanner.setStopKey(end);
    scanner.setFamily(NAME_FAMILY);
    scanner.setQualifier(BRANCH_QUALIFIER);
    
    // set the regex filter
    
    // we want one branch below the current ID so we want something like:
    // {0, 1, 1, 2, 3, 4 }  where { 0, 1 } is the tree ID, { 1, 2, 3, 4 } is the 
    // branch
    // "^\\Q\000\001\001\002\003\004\\E(?:.{4})$"
    
    final StringBuilder buf = new StringBuilder((start.length * 6) + 20);
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
        + "^\\Q");
    for (final byte b : start) {
      buf.append((char) (b & 0xFF));
    }
    buf.append("\\E(?:.{").append(INT_WIDTH).append("})$");
    
    scanner.setKeyRegexp(buf.toString(), CHARSET);
    return scanner;
  }
  
  // GETTERS AND SETTERS ----------------------------
  
  /** @return The ID of the tree this branch belongs to */
  public int getTreeId() {
    return tree_id;
  }

  /** @return The ID of this branch */
  public String getBranchId() {
    return UniqueId.uidToString(compileBranchId());
  }
  
  /** @return The path of the tree */
  public Map<Integer, String> getPath() {
    compileBranchId();
    return path;
  }

  /** @return Depth of this branch */
  public int getDepth() {
    return path.lastKey();
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
    return branches;
  }

  /** @param tree_id ID of the tree this branch belongs to */
  public void setTreeId(int tree_id) {
    this.tree_id = tree_id;
  }
  
  /** @param display_name Public name to display */
  public void setDisplayName(String display_name) {
    this.display_name = display_name;
  }

 }

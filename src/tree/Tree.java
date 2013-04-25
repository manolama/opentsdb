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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * 
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class Tree {
  private static final Logger LOG = LoggerFactory.getLogger(Tree.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** UID table row where tree definitions are stored */
  private static byte[] TREE_DEFINITION_ROW = { 0x01, 0x00 };
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  /** Byte prefix for collision columns */
  private static byte COLLISION_PREFIX = 0x01;
  /** Byte prefix for not matched columns */
  private static byte NOT_MATCHED_PREFIX = 0x02;
  
  /** Type reference for collisions and no matches */
  private static TypeReference<HashSet<String>> SETS_TYPE_REF =
    new TypeReference<HashSet<String>>() {};
    
  /** Unique ID of the tree in the system*/
  protected int tree_id;
  
  /** Name of the tree */
  protected String name = "";
  
  /** A brief description of the tree */
  protected String description = "";
  
  /** Notes about the tree */
  protected String notes = "";
  
  /** Whether or not strict matching is enabled */
  protected boolean strict_match;
  
  /** Sorted, two dimensional map of the tree's rules */
  protected TreeMap<Integer, TreeMap<Integer, TreeRule>> rules;

  /** List of non-matched TSUIDs that were not included in the tree */
  protected HashSet<String> not_matched;
  
  /** List of TSUID collisions that were not included in the tree */
  protected HashSet<String> collisions;

  /** Unix time, in seconds, when the tree was created */
  protected long created;
  
  /** Last time the tree was updated with an entry */
  protected long last_update;

  /** Character used to separate branches/leaves in the tree results */
  protected char node_separator = '|';
  
  /** Tracks fields that have changed by the user to avoid overwrites */
  protected final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();
  
  /**
   * Default constructor necessary for de/serialization
   */
  public Tree() {
    initializeChangedMap();
  }
  
  /**
   * Constructor that sets the tree ID
   * @param tree_id ID of this tree
   */
  public Tree(final int tree_id) {
    this.tree_id = tree_id;
    this.created = System.currentTimeMillis() / 1000;
    initializeChangedMap();
  }
  
  /** @return Information about the tree */
  @Override
  public String toString() {
    return "treeId: " + tree_id + " name: " + name;
  }
  
  /**
   * Copies changes from the incoming tree into the local tree, overriding if
   * called to. Only parses user mutable fields, excluding rules.
   * @param tree The tree to copy from
   * @param overwrite Whether or not to copy all values from the incoming tree
   * @return True if there were changes, false if not
   */
  public boolean copyChanges(final Tree tree, final boolean overwrite) {
    if (tree == null) {
      throw new IllegalArgumentException("Cannot copy a null tree");
    }
    if (tree_id != tree.tree_id) {
      throw new IllegalArgumentException("Tree IDs do not match");
    }
    
    if (overwrite || tree.changed.get("name")) {
      name = tree.name;
      changed.put("name", true);
    }
    if (overwrite || tree.changed.get("description")) {
      description = tree.description;
      changed.put("description", true);
    }
    if (overwrite || tree.changed.get("notes")) {
      notes = tree.notes;
      changed.put("notes", true);
    }
    if (overwrite || tree.changed.get("strict_match")) {
      strict_match = tree.strict_match;
      changed.put("strict_match", true);
    }
    if (overwrite || tree.changed.get("node_separator")) {
      node_separator = tree.node_separator;
      changed.put("node_separator", true);
    }
    for (boolean has_changes : changed.values()) {
      if (has_changes) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Adds the given rule to the tree, replacing anything in the designated spot
   * @param rule The rule to add
   */
  public void addRule(final TreeRule rule) {
    if (rule == null) {
      throw new IllegalArgumentException("Null rules are not accepted");
    }
    if (rules == null) {
      rules = new TreeMap<Integer, TreeMap<Integer, TreeRule>>();
    }
    
    TreeMap<Integer, TreeRule> level = rules.get(rule.getLevel());
    if (level == null) {
      level = new TreeMap<Integer, TreeRule>();
      level.put(rule.getOrder(), rule);
      rules.put(rule.getLevel(), level);
    } else {
      level.put(rule.getOrder(), rule);
    }
    
    changed.put("rules", true);
  }
  
  /**
   * Replaces all of the rules with the rules in the array list.
   * @param rules The list of rules to set
   */
  public void replaceRules(final ArrayList<TreeRule> rules) {
    if (rules == null) {
      throw new IllegalArgumentException(
          "Null rules are not accepted, empty lists are OK");
    }
    
    this.rules = new TreeMap<Integer, TreeMap<Integer, TreeRule>>();
    for (final TreeRule rule : rules) {
      addRule(rule);
    }
  }
  
  /**
   * Adds a TSUID to the collision local list, must then be synced with storage
   * @param tsuid TSUID to add to the set
   */
  public void addCollision(final String tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null collisions not allowed");
    }
    if (collisions == null) {
      collisions = new HashSet<String>();
    }
    if (!collisions.contains(tsuid)) {
      collisions.add(tsuid);
      changed.put("collisions", true);
    }
  }
  
  /**
   * Adds a TSUID to the not matched local list when strict_matching is enabled.
   * Must be synced with storage
   * @param tsuid TSUID to add to the set
   */
  public void addNotMatched(final String tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null non matches not allowed");
    }
    if (not_matched == null) {
      not_matched = new HashSet<String>();
    }
    if (!not_matched.contains(tsuid)) {
      not_matched.add(tsuid);
      changed.put("not_matched", true);
    }
  }
  
  /**
   * Attempts to store the tree
   * @param tsdb The TSDB to use for access
   * @param lock An optional lock to use on the row
   * @throws IllegalArgumentException if the Tree ID is missing
   * @throws HBaseException if a storage exception occurred
   */
  public void storeTree(final TSDB tsdb, final RowLock lock) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    // if there aren't any changes, save time and bandwidth by not writing to
    // storage
    boolean has_tree_changes = false;
    boolean has_set_changes = false;
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        if (entry.getKey().equals("collisions") || 
            entry.getKey().equals("not_matched")) {
          has_set_changes = true;
        } else {
          has_tree_changes = true;
        }
      }
    }
    if (!has_tree_changes && !has_set_changes) {
      LOG.trace(this + " does not have changes, skipping sync to storage");
      throw new IllegalStateException("No changes detected in the tree");
    }
    
    if (has_tree_changes) {
      final byte[] qualifier = new byte[] { ((Integer)tree_id).byteValue() };
      final PutRequest put = new PutRequest(tsdb.uidTable(), TREE_DEFINITION_ROW, 
          NAME_FAMILY, qualifier, JSON.serializeToBytes(this), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
    }
    
    if (has_set_changes) {
      // sync collisions and not matched
      syncCollisions(tsdb, lock);
      syncNotMatched(tsdb, lock);
    }
    
    // reset the change map so we don't keep writing
    initializeChangedMap();
  }
  
  /**
   * Retrieves a single rule from the rule set given a level and order
   * @param level The level where the rule resides
   * @param order The order in the level where the rule resides
   * @return The rule if found, null if not found
   */
  public TreeRule getRule(final int level, final int order) {
    if (rules == null || rules.isEmpty()) { 
      return null;
    }
    
    TreeMap<Integer, TreeRule> rule_level = rules.get(level);
    if (rule_level == null || rule_level.isEmpty()) {
      return null;
    }
    
    return rule_level.get(order);
  }
  
  /**
   * Attempts to synchronize changes to a tree's meta data to storage by first
   * fetching the tree, applying changes and saving. If no changes were found
   * it will throw an IllegalStateException.
   * @param tsdb The TSDB to use for storage access
   * @param tree The incoming tree with changes
   * @param overwrite Whether or not to overwrite all user mutable fields
   * @return The synchronized tree
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws IllegalStateException if there weren't any changes to save
   * @throws HBaseException if there's a problem with storage
   */
  public static Tree syncToStorage(final TSDB tsdb, final Tree tree, 
      final boolean overwrite) {
    if (tree.tree_id < 1 || tree.tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final RowLock lock = tsdb.hbaseAcquireLock(tsdb.uidTable(), 
        TREE_DEFINITION_ROW, (short)3);
    try {
      final Tree stored_tree = fetchTree(tsdb, tree.tree_id, lock);
      if (stored_tree == null) {
        return null;
      }
      
      if (!stored_tree.copyChanges(tree, overwrite)) {
        LOG.debug(stored_tree + 
            " does not have changes, skipping sync to storage");
        throw new IllegalStateException("No changes detected in tree meta data");
      }
      
      stored_tree.storeTree(tsdb, lock);
      return stored_tree;
    } finally {
      // don't forget to release the lock!
      tsdb.getClient().unlockRow(lock);
    }
  }
  
  /**
   * Attempts to create a new tree with the information provided
   * @param tsdb The TSDB to use for storage access
   * @param tree The incoming tree with data to store
   * @return The new tree
   * @throws IllegalStateException if we've used up all of the tree IDs
   * @throws HBaseException if there's a problem with storage
   */
  public static Tree createNewTree(final TSDB tsdb, final Tree tree) {
    final RowLock lock = tsdb.hbaseAcquireLock(tsdb.uidTable(), 
        TREE_DEFINITION_ROW, (short)3);
    
    try {
      final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
      get.family(NAME_FAMILY);
      get.withRowLock(lock);
      
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      int id = 0;
      if (row == null) {
        id = 1;
      } else {
        for (KeyValue column : row){
          // skip the other column types, we only want single byte IDs
          if (column.qualifier() == null || column.qualifier().length > 1) {
            continue;
          }
          
          // convert
          final byte[] four_byte = { 0x00, 0x00, 0x00, column.qualifier()[0]};
          final int column_id = Bytes.getInt(four_byte);
          if (column_id > id) {
            id = column_id;
          }
        }
  
        if (id >= 254) {
          throw new IllegalStateException(
            "Reached the maximum number of tree definitions");
        }
        id++;
      }
      
      tree.setTreeId(id);
      tree.storeTree(tsdb, lock);
      return tree;
    } catch (HBaseException e) {
      throw e;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    } finally {
      // don't forget to unlock the row!
      tsdb.getClient().unlockRow(lock);
    }
  }

  /**
   * Attempts to fetch the given tree from storage without a lock
   * @param tsdb The TSDB to use for access
   * @param tree_id The Tree to fetch
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws HBaseException if a storage exception occurred
   */
  public static Tree fetchTree(final TSDB tsdb, final int tree_id) {
    return fetchTree(tsdb, tree_id, null);
  }
  
  /**
   * Attempts to fetch the given tree from storage
   * @param tsdb The TSDB to use for access
   * @param tree_id The Tree to fetch
   * @param lock An optional lock for atomic updates
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws HBaseException if a storage exception occurred
   */
  public static Tree fetchTree(final TSDB tsdb, final int tree_id, 
      final RowLock lock) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    final byte[] qualifier = new byte[] { ((Integer)tree_id).byteValue() }; 
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    get.qualifier(qualifier);
    if (lock != null) {
      get.withRowLock(lock);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      final Tree stored_tree = 
        JSON.parseToObject(row.get(0).value(), Tree.class);
      // deserialization uses the setters, hence it sets the changed map, reset
      stored_tree.initializeChangedMap();
      return stored_tree;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Attempts to retreive all trees from the tree definition row. If the row
   * doesn't exist and no trees were found, returns an empty list.
   * @param tsdb The TSDB to use for storage
   * @return A list of tree objects. May be empty if none were found
   */
  public static List<Tree> fetchAllTrees(final TSDB tsdb) {
    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    
    try {
      final ArrayList<Tree> trees = new ArrayList<Tree>();
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return trees;
      }

      for (KeyValue column : row) {
        if (column.qualifier().length == 1) {
          trees.add(JSON.parseToObject(column.value(), Tree.class));
        }
      }
      
      return trees;   
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Returns the collision set from storage for the given tree
   * @param tsdb TSDB to use for storage access
   * @param tree_id ID of the tree to fetch collisions for
   * @return A list of collisions or null if nothing was stored
   */
  public static HashSet<String> fetchCollisions(final TSDB tsdb, 
      final int tree_id) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] qualifier = new byte[] { COLLISION_PREFIX, 
        ((Integer)tree_id).byteValue() };
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    get.qualifier(qualifier);
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row != null && !row.isEmpty()) {
        return JSON.parseToObject(row.get(0).value(), SETS_TYPE_REF);
      } else {
        return null;
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    } 
  }
  
  /**
   * Returns the not matched set from storage for the given tree
   * @param tsdb TSDB to use for storage access
   * @param tree_id ID of the tree to fetch non matches for
   * @return A list of non matched TSUIDS or null if nothing was stored
   */
  public static HashSet<String> fetchNotMatched(final TSDB tsdb, 
      final int tree_id) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] qualifier = new byte[] { NOT_MATCHED_PREFIX, 
        ((Integer)tree_id).byteValue() };
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    get.qualifier(qualifier);
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row != null && !row.isEmpty()) {
        return JSON.parseToObject(row.get(0).value(), SETS_TYPE_REF);
      } else {
        return null;
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    } 
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    // tree_id can't change
    changed.put("name", false);
    changed.put("field", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("strict_match", false);
    changed.put("rules", false);
    changed.put("not_matched", false);
    changed.put("collisions", false);
    changed.put("created", false);
    changed.put("last_update", false);
    changed.put("version", false);
    changed.put("node_separator", false);
  }
  
  /**
   * Loads the collision set from storage and merges the local set into the
   * storage set, then saves back to storage.
   * @param tsdb TSDB to use for storage access
   * @param lock A valid row lock
   * @throws HBaseException if something went pear shaped
   */
  private void syncCollisions(final TSDB tsdb, final RowLock lock) {
    // if we don't have any changes or any data, don't bother calling
    if (!changed.containsKey("collisions") || !changed.get("collisions") ||
        collisions == null || collisions.isEmpty()) {
      return;
    }
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    if (lock == null) {
      throw new IllegalArgumentException("Attempting to sync without a lock");
    }
    
    final byte[] qualifier = new byte[] { COLLISION_PREFIX, 
        ((Integer)tree_id).byteValue() };
    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    get.qualifier(qualifier);
    get.withRowLock(lock);
    try {
      HashSet<String> stored_collisions = null;
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row != null && !row.isEmpty()) {
        stored_collisions = JSON.parseToObject(row.get(0).value(), 
            SETS_TYPE_REF);
        stored_collisions.addAll(collisions);
      } else {
        stored_collisions = collisions;
      }
      
      final PutRequest put = new PutRequest(tsdb.uidTable(), 
          TREE_DEFINITION_ROW, NAME_FAMILY, qualifier, 
          JSON.serializeToBytes(stored_collisions), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
      
      // flush the local collisions so we don't eat up memory
      collisions = null;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Loads the non matched TSUID set from storage and merges the local set into
   * the storage set, then saves back to storage.
   * @param tsdb TSDB to use for storage access
   * @param lock A valid row lock
   * @throws HBaseException if something went pear shaped
   */
  private void syncNotMatched(final TSDB tsdb, final RowLock lock) {
    // if we don't have any changes or any data, don't bother calling
    if (!changed.containsKey("not_matched") || !changed.get("not_matched") ||
        not_matched == null || not_matched.isEmpty()) {
      return;
    }
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    if (lock == null) {
      throw new IllegalArgumentException("Attempting to sync without a lock");
    }
    
    // No match qualifiers have a #2 prefix
    final byte[] qualifier = new byte[] { NOT_MATCHED_PREFIX, 
        ((Integer)tree_id).byteValue() };

    final GetRequest get = new GetRequest(tsdb.uidTable(), TREE_DEFINITION_ROW);
    get.family(NAME_FAMILY);
    get.qualifier(qualifier);
    get.withRowLock(lock);
    try {
      HashSet<String> stored_not_matched = null;
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row != null && !row.isEmpty()) {
        stored_not_matched = JSON.parseToObject(row.get(0).value(), 
            SETS_TYPE_REF);
        stored_not_matched.addAll(not_matched);
      } else {
        stored_not_matched = not_matched;
      }
      
      final PutRequest put = new PutRequest(tsdb.uidTable(), 
          TREE_DEFINITION_ROW, NAME_FAMILY, qualifier, 
          JSON.serializeToBytes(stored_not_matched), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
      
      // flush the local not_matched so we don't eat up memory
      not_matched = null;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  // GETTERS AND SETTERS ----------------------------
  
  /** @return The treeId */
  public int getTreeId() {
    return tree_id;
  }

  /** @return The name of the tree */
  public String getName() {
    return name;
  }

  /** @return An optional description of the tree */
  public String getDescription() {
    return description;
  }

  /** @return Optional notes about the tree */
  public String getNotes() {
    return notes;
  }

  /** @return Whether or not strict matching is enabled */
  public boolean getStrictMatch() {
    return strict_match;
  }

  /** @return The tree's rule set */
  public Map<Integer, TreeMap<Integer, TreeRule>> getRules() {
    return rules;
  }

  /** @return List of TSUIDs that did not match any rules */
  @JsonIgnore
  public Set<String> getNotMatched() {
    return not_matched;
  }

  /** @return List of TSUIDs that were not stored due to collisions */
  @JsonIgnore
  public Set<String> getCollisions() {
    return collisions;
  }

  /** @return When the tree was created, Unix epoch in seconds */
  public long getCreated() {
    return created;
  }

  /** @return Last time the tree was updated, Unix epoch in seconds */
  public long getLastUpdate() {
    return last_update;
  }

  /** @return Tree node separator character */
  public char getNodeSeparator() {
    return node_separator;
  }

  /** @param name A descriptive name for the tree */
  public void setName(String name) {
    if (!this.name.equals(name)) {
      changed.put("name", true);
      this.name = name;
    }
  }

  /** @param description A brief description of the tree */
  public void setDescription(String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes Optional notes about the tree */
  public void setNotes(String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param strict_match Whether or not a TSUID must match all rules in the
   * tree to be included */
  public void setStrictMatch(boolean strict_match) {
    if (this.strict_match != strict_match) {
      changed.put("strict_match", true);
      this.strict_match = strict_match;
    }
  }

  /** @param node_separator Node separation character */
  public void setNodeSeparator(char node_separator) {
    if (this.node_separator != node_separator) {
      changed.put("node_separator", true);
      this.node_separator = node_separator;
    }
  }
 
  /** @param treeId ID of the tree, users cannot modify this */
  public void setTreeId(int treeId) {
    this.tree_id = treeId;
  }

  /** @param created The time when this tree was created, 
   * Unix epoch in seconds */
  public void setCreated(long created) {
    this.created = created;
  }

  /** @param last_update Last time the tree was modified, 
   * Unix epoch in seconds */
  public void setLastUpdate(long last_update) {
    if (this.last_update != last_update) {
      changed.put("last_update", true);
      this.last_update = last_update;
    }
  }

}

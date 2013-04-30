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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * 
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class Tree {
  private static final Logger LOG = LoggerFactory.getLogger(Tree.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** Width of tree IDs in bytes */
  private static final short TREE_ID_WIDTH = 2;
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  /** The tree qualifier */
  private static final byte[] TREE_QUALIFIER = "tree".getBytes(CHARSET);
  /** Integer width in bytes */
  private static final short INT_WIDTH = 4;
  /** Byte prefix for collision columns */
  private static byte COLLISION_SUFFIX = 0x01;
  private static byte[] COLLISION_PREFIX = "tree_collision:".getBytes(CHARSET);
  /** Byte prefix for not matched columns */
  private static byte NOT_MATCHED_SUFFIX = 0x02;
  private static byte[] NOT_MATCHED_PREFIX = "tree_not_matched:".getBytes(CHARSET);

  private int tree_id;
  
  /** Name of the tree */
  private String name = "";
  
  /** A brief description of the tree */
  private String description = "";
  
  /** Notes about the tree */
  private String notes = "";
  
  /** Whether or not strict matching is enabled */
  private boolean strict_match;
  
  /** Sorted, two dimensional map of the tree's rules */
  private TreeMap<Integer, TreeMap<Integer, TreeRule>> rules;

  /** List of non-matched TSUIDs that were not included in the tree */
  private HashMap<String, String> not_matched;
  
  /** List of TSUID collisions that were not included in the tree */
  private HashMap<String, String> collisions;

  /** Unix time, in seconds, when the tree was created */
  private long created;

  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
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
   * Adds a TSUID to the collision local list, must then be synced with storage
   * @param tsuid TSUID to add to the set
   */
  public void addCollision(final String tsuid, final String existing_tsuid) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null collisions not allowed");
    }
    if (collisions == null) {
      collisions = new HashMap<String, String>();
    }
    if (!collisions.containsKey(tsuid)) {
      collisions.put(tsuid, existing_tsuid);
      changed.put("collisions", true);
    }
  }
  
  /**
   * Adds a TSUID to the not matched local list when strict_matching is enabled.
   * Must be synced with storage
   * @param tsuid TSUID to add to the set
   */
  public void addNotMatched(final String tsuid, final String message) {
    if (tsuid == null || tsuid.isEmpty()) {
      throw new IllegalArgumentException("Empty or null non matches not allowed");
    }
    if (not_matched == null) {
      not_matched = new HashMap<String, String>();
    }
    if (!not_matched.containsKey(tsuid)) {
      not_matched.put(tsuid, message);
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
  public Deferred<Boolean> storeTree(final TSDB tsdb, final boolean overwrite) {
    if (tree_id < 1 || tree_id > 65535) {
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

    if (has_set_changes) {
      // sync collisions and not matched
      flushCollisions(tsdb);
      flushNotMatched(tsdb);
    }
    
    if (has_tree_changes) {
      // fetch the stored value first to use in the CAS call
      Tree stored_tree = fetchTree(tsdb, tree_id);
      final byte[] original_tree = stored_tree == null ? new byte[0] : 
        stored_tree.toStorageJson();
      
      // now copy changes
      if (stored_tree == null) {
        stored_tree = this;
      } else {
        stored_tree.copyChanges(this, overwrite);
      }
      
      // reset the change map so we don't keep writing
      initializeChangedMap();
      
      final PutRequest put = new PutRequest(tsdb.uidTable(), 
          Tree.idToBytes(tree_id), NAME_FAMILY, TREE_QUALIFIER, 
          stored_tree.toStorageJson());
      return tsdb.getClient().compareAndSet(put, original_tree);
    }
    
    return Deferred.fromResult(true);
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
  
  public int createNewTree(final TSDB tsdb) {
    List<Tree> trees = fetchAllTrees(tsdb);
    int max_id = 0;
    if (trees != null) {
      for (Tree tree : trees) {
        System.out.println("Tree: " + tree.tree_id);
        if (tree.tree_id > max_id) {
          max_id = tree.tree_id;
        }
      }
    }
    
    tree_id = max_id + 1;
    System.out.println("Found: " + tree_id + " trees");
    if (tree_id > 65535) {
      throw new IllegalStateException("Exhausted all Tree IDs");
    }
    try {
      if (storeTree(tsdb, true).joinUninterruptibly()) {
        return tree_id;
      } else {
        return 0;
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (IllegalStateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
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
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }

    // fetch the whole row
    final GetRequest get = new GetRequest(tsdb.uidTable(), idToBytes(tree_id));
    get.family(NAME_FAMILY);
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      
      final Tree tree = new Tree();
      tree.setTreeId(tree_id);
      for (KeyValue column : row) {
        if (Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
          // it's *this* tree. We deserialize to a new object and copy
          // since the columns could be in any order and we may get a rule 
          // before the tree object
          final Tree local_tree = JSON.parseToObject(column.value(), Tree.class);
          tree.created = local_tree.created;
          tree.description = local_tree.description;
          tree.name = local_tree.name;
          tree.notes = local_tree.notes;
          tree.strict_match = tree.strict_match;
        } else if (Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0, 
            TreeRule.RULE_PREFIX().length) == 0) {
          final TreeRule rule = TreeRule.parseFromStorage(column);
          tree.addRule(rule);
        }
      }
      return tree;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public static boolean treeExists(final TSDB tsdb, final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }

    // fetch the whole row
    final GetRequest get = new GetRequest(tsdb.uidTable(), idToBytes(tree_id));
    get.family(NAME_FAMILY);
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return false;
      }

      for (KeyValue column : row) {
        if (Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
          return true;
        } 
      }
      return false;
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
    final Scanner scanner = setupScanner(tsdb);
    
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      final ArrayList<Tree> trees = new ArrayList<Tree>();
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        System.out.println("Rows: " + rows.size());
        for (final ArrayList<KeyValue> row : rows) {
          if (row.isEmpty()) {
            continue;
          }
          
          Tree tree = new Tree();
          System.out.println("Columns: " + row.size() +  "  Key: " + tree.tree_id);
          for (KeyValue column : row) {
            if (column.qualifier().length >= TREE_QUALIFIER.length && 
                Bytes.memcmp(TREE_QUALIFIER, column.qualifier()) == 0) {
              // it's *this* tree. We deserialize to a new object and copy
              // since the columns could be in any order and we may get a rule 
              // before the tree object
              final Tree local_tree = JSON.parseToObject(column.value(), 
                  Tree.class);
              tree.created = local_tree.created;
              tree.description = local_tree.description;
              tree.name = local_tree.name;
              tree.notes = local_tree.notes;
              tree.strict_match = tree.strict_match;
              tree.setTreeId(bytesToId(row.get(0).key()));
            } else if (column.qualifier().length > TreeRule.RULE_PREFIX().length &&
                Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 
                0, TreeRule.RULE_PREFIX().length) == 0) {
              final TreeRule rule = TreeRule.parseFromStorage(column);
              tree.addRule(rule);
            } else {
              System.out.println("No tree in row: " + UniqueId.uidToString(column.key()));
            }
          }
          if (tree.tree_id > 0) {
            trees.add(tree);
          }
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
  public static Map<String, String> fetchCollisions(final TSDB tsdb, 
      final int tree_id, final List<String> tsuids) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_SUFFIX;
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), row_key);
    get.family(NAME_FAMILY);
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[COLLISION_PREFIX.length + 
                                          (tsuid.length() / 2)];
        System.arraycopy(COLLISION_PREFIX, 0, qualifier, 0, 
            COLLISION_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, COLLISION_PREFIX.length, 
            tsuid_bytes.length);
        qualifiers[index] = qualifier;
        System.out.println("Set Q: " + new String(qualifier, CHARSET));
        index++;
      }
      get.qualifiers(qualifiers);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return new HashMap<String, String>();
      }
      System.out.println("Rows: " + row.size());
      final HashMap<String, String> collisions = 
        new HashMap<String, String>(row.size());
      for (KeyValue column : row) {
        System.out.println("Qual: " + new String(column.qualifier(), CHARSET));
        if (column.qualifier().length > COLLISION_PREFIX.length && 
            Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0, 
                COLLISION_PREFIX.length) == 0) {
          final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(), 
              COLLISION_PREFIX.length, column.qualifier().length);
          System.out.println("Parsed: " + UniqueId.uidToString(parsed_tsuid));
          collisions.put(UniqueId.uidToString(parsed_tsuid), 
              new String(column.value(), CHARSET));
        }
      }
      
      return collisions;
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
  public static Map<String, String> fetchNotMatched(final TSDB tsdb, 
      final int tree_id, final List<String> tsuids) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_SUFFIX;
    
    final GetRequest get = new GetRequest(tsdb.uidTable(), row_key);
    get.family(NAME_FAMILY);
    if (tsuids != null && !tsuids.isEmpty()) {
      final byte[][] qualifiers = new byte[tsuids.size()][];
      int index = 0;
      for (String tsuid : tsuids) {
        final byte[] qualifier = new byte[NOT_MATCHED_PREFIX.length + 
                                          (tsuid.length() / 2)];
        System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifier, 0, 
            NOT_MATCHED_PREFIX.length);
        final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
        System.arraycopy(tsuid_bytes, 0, qualifier, NOT_MATCHED_PREFIX.length, 
            tsuid_bytes.length);
        qualifiers[index] = qualifier;
        System.out.println("Set Q: " + new String(qualifier, CHARSET));
        index++;
      }
      get.qualifiers(qualifiers);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return new HashMap<String, String>();
      }
      
      HashMap<String, String> not_matched = new HashMap<String, String>(row.size());
      for (KeyValue column : row) {
        final byte[] parsed_tsuid = Arrays.copyOfRange(column.qualifier(), 
            NOT_MATCHED_PREFIX.length, column.qualifier().length);
        not_matched.put(UniqueId.uidToString(parsed_tsuid), 
            new String(column.value(), CHARSET));
      }
      
      return not_matched;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    } 
  }
  
  public static Deferred<ArrayList<Object>> deleteTree(final TSDB tsdb, 
      final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    // confirm the tree exists. If it doesn't use the fsck utility to clean up
    if (!treeExists(tsdb, tree_id)) {
      throw new IllegalArgumentException("Unable to locate tree: " + tree_id);
    }
    
    final ArrayList<Deferred<Object>> results = 
      new ArrayList<Deferred<Object>>();
    
    // scan all of the rows starting with the tree ID. We can't just delete the
    // rows as there may be other types of data. Thus we have to check the
    // qualifiers of every column to see if it's safe to delete
    final byte[] start = idToBytes(tree_id);
    final byte[] end = idToBytes(tree_id + 1);
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start);
    scanner.setStopKey(end);   
    scanner.setFamily(NAME_FAMILY);
    
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          // one delete request per row. We'll almost always delete the whole
          // row, so preallocate some ram.
          ArrayList<byte[]> qualifiers = new ArrayList<byte[]>(row.size());
          for (KeyValue column : row) {
            // tree
            if (Bytes.equals(TREE_QUALIFIER, column.qualifier())) {
              LOG.trace("Deleting tree defnition in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // branches
            } else if (Bytes.equals(Branch.BRANCH_QUALIFIER(), column.qualifier())) {
              LOG.trace("Deleting branch in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
            
            // leaves
            } else if (column.qualifier().length > Leaf.LEAF_PREFIX().length &&
                Bytes.memcmp(Leaf.LEAF_PREFIX(), column.qualifier(), 0, 
                    Leaf.LEAF_PREFIX().length) == 0) {
              LOG.trace("Deleting leaf in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // collisions
            } else if (column.qualifier().length > COLLISION_PREFIX.length && 
                Bytes.memcmp(COLLISION_PREFIX, column.qualifier(), 0, 
                    COLLISION_PREFIX.length) == 0) {
              LOG.trace("Deleting collision in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // not matched
            } else if (column.qualifier().length > NOT_MATCHED_PREFIX.length && 
                Bytes.memcmp(NOT_MATCHED_PREFIX, column.qualifier(), 0, 
                    NOT_MATCHED_PREFIX.length) == 0) {
              LOG.trace("Deleting not matched in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
              
            // tree rule
            } else if (column.qualifier().length > TreeRule.RULE_PREFIX().length && 
                Bytes.memcmp(TreeRule.RULE_PREFIX(), column.qualifier(), 0, 
                    TreeRule.RULE_PREFIX().length) == 0) {
              LOG.trace("Deleting tree rule in row: " + 
                  Branch.idToString(column.key()));
              qualifiers.add(column.qualifier());
            } 
          }
          
          if (qualifiers.size() > 0) {
            final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
                row.get(0).key(), NAME_FAMILY, 
                qualifiers.toArray(new byte[qualifiers.size()][])
                );
            results.add(tsdb.getClient().delete(delete));
          }
        }
      }
      
      return Deferred.group(results);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Converts the tree ID into a byte array TREE_ID_WIDTH in size
   * @param tree_id The tree ID to convert
   * @return The tree ID as a byte array
   */
  public static byte[] idToBytes(final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Missing or invalid tree ID");
    }
    final byte[] id = Bytes.fromInt(tree_id);
    return Arrays.copyOfRange(id, id.length - TREE_ID_WIDTH, id.length);
  }
  
  public static int bytesToId(final byte[] row_key) {
    if (row_key.length < TREE_ID_WIDTH) {
      throw new IllegalArgumentException("Row key was less than " + 
          TREE_ID_WIDTH + " in length");
    }
    
    final byte[] tree_id = new byte[INT_WIDTH];
    System.arraycopy(row_key, 0, tree_id, INT_WIDTH - Tree.TREE_ID_WIDTH(), 
        Tree.TREE_ID_WIDTH());
    return Bytes.getInt(tree_id);    
  }
  
  public static byte[] COLLISION_PREFIX() {
    return COLLISION_PREFIX;
  }
  
  public static byte[] NOT_MATCHED_PREFIX() {
    return NOT_MATCHED_PREFIX;
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
  private void flushCollisions(final TSDB tsdb) {
    // if we don't have any changes or any data, don't bother calling
    if (!changed.containsKey("collisions") || !changed.get("collisions") ||
        collisions == null || collisions.isEmpty()) {
      return;
    }
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = COLLISION_SUFFIX;

    try{
      for (Map.Entry<String, String> entry : collisions.entrySet()) {
        final byte[] qualifier = new byte[COLLISION_PREFIX.length + 
                                          (entry.getKey().length() / 2)];
        System.arraycopy(COLLISION_PREFIX, 0, qualifier, 0, 
            COLLISION_PREFIX.length);
        final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
        System.arraycopy(tsuid, 0, qualifier, COLLISION_PREFIX.length, 
            tsuid.length);
        
        final PutRequest put = new PutRequest(tsdb.uidTable(), row_key, 
            NAME_FAMILY, qualifier, entry.getValue().getBytes(CHARSET));
        tsdb.getClient().compareAndSet(put, new byte[0]);
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
    collisions.clear();
  }
  
  /**
   * Loads the non matched TSUID set from storage and merges the local set into
   * the storage set, then saves back to storage.
   * @param tsdb TSDB to use for storage access
   * @param lock A valid row lock
   * @throws HBaseException if something went pear shaped
   */
  private void flushNotMatched(final TSDB tsdb) {
    // if we don't have any changes or any data, don't bother calling
    if (!changed.containsKey("not_matched") || !changed.get("not_matched") ||
        not_matched == null || not_matched.isEmpty()) {
      return;
    }
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    final byte[] row_key = new byte[TREE_ID_WIDTH + 1];
    System.arraycopy(idToBytes(tree_id), 0, row_key, 0, TREE_ID_WIDTH);
    row_key[TREE_ID_WIDTH] = NOT_MATCHED_SUFFIX;

    try{
      for (Map.Entry<String, String> entry : not_matched.entrySet()) {
        final byte[] qualifier = new byte[NOT_MATCHED_PREFIX.length + 
                                          (entry.getKey().length() / 2)];
        System.arraycopy(NOT_MATCHED_PREFIX, 0, qualifier, 0, 
            NOT_MATCHED_PREFIX.length);
        final byte[] tsuid = UniqueId.stringToUid(entry.getKey());
        System.arraycopy(tsuid, 0, qualifier, NOT_MATCHED_PREFIX.length, 
            tsuid.length);
        
        final PutRequest put = new PutRequest(tsdb.uidTable(), row_key, 
            NAME_FAMILY, qualifier, entry.getValue().getBytes(CHARSET));
        tsdb.getClient().compareAndSet(put, new byte[0]);
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
    not_matched.clear();
  }
  
  private byte[] toStorageJson() {
    // TODO - precalc how much memory to grab
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      final JsonGenerator json = JSON.getFactory().createGenerator(output);
      
      json.writeStartObject();
      
      // we only need to write a small amount of information
      json.writeNumberField("treeId", tree_id);
      json.writeStringField("name", name);
      json.writeStringField("description", description);
      json.writeStringField("notes", notes);
      json.writeBooleanField("strictMatch", strict_match);
      json.writeNumberField("created", created);
      
      json.writeEndObject();
      json.close();
      
      // TODO zero copy?
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static Scanner setupScanner(final TSDB tsdb) {
    final byte[] start = new byte[TREE_ID_WIDTH];
    final byte[] end = new byte[TREE_ID_WIDTH];
    Arrays.fill(end, (byte)0xFF);
    final Scanner scanner = tsdb.getClient().newScanner(tsdb.uidTable());
    scanner.setStartKey(start);
    scanner.setStopKey(end);   
    scanner.setFamily(NAME_FAMILY);
    
    // set the filter to match only on TREE_ID_WIDTH row keys
    final StringBuilder buf = new StringBuilder(20);
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
        + "^\\Q");
    buf.append("\\E(?:.{").append(TREE_ID_WIDTH).append("})$");
    scanner.setKeyRegexp(buf.toString(), CHARSET);
    return scanner;
  }
  
  // GETTERS AND SETTERS ----------------------------
  
  public static int TREE_ID_WIDTH() {
    return TREE_ID_WIDTH;
  }
  
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
  public Map<String, String> getNotMatched() {
    return not_matched;
  }

  /** @return List of TSUIDs that were not stored due to collisions */
  @JsonIgnore
  public Map<String, String> getCollisions() {
    return collisions;
  }

  /** @return When the tree was created, Unix epoch in seconds */
  public long getCreated() {
    return created;
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

  /** @param treeId ID of the tree, users cannot modify this */
  public void setTreeId(int treeId) {
    this.tree_id = treeId;
  }

  /** @param created The time when this tree was created, 
   * Unix epoch in seconds */
  public void setCreated(long created) {
    this.created = created;
  }

}

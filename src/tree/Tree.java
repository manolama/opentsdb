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

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

/**
 * Represents user accessible fields 
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class Tree {

  /** Unique ID of the tree in the system*/
  protected int treeId;
  
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
  
  /** Total number of branches in the tree */
  protected int total_branches;
  
  /** Total number of leaves in the tree */
  protected int total_leaves;
  
  /** Unix time, in seconds, when the tree was created */
  protected long created;
  
  /** Last time the tree was updated with an entry */
  protected long last_update;
  
  /** Current version of the tree, used to cull stale branches and leaves */
  protected long version;

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
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    // tree_id can't change
    changed.put("name", false);
    changed.put("field", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("rules", false);
    changed.put("not_matched", false);
    changed.put("collisions", false);
    changed.put("total_branches", false);
    changed.put("total_leaves", false);
    changed.put("created", false);
    changed.put("last_update", false);
    changed.put("version", false);
    changed.put("node_separator", false);
  }

  // GETTERS AND SETTERS ----------------------------
  
  /** @return The treeId */
  public int getTreeId() {
    return treeId;
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
  public TreeMap<Integer, TreeMap<Integer, TreeRule>> getRules() {
    return rules;
  }

  /** @return List of TSUIDs that did not match any rules */
  public HashSet<String> getNotMatched() {
    return not_matched;
  }

  /** @return List of TSUIDs that were not stored due to collisions */
  public HashSet<String> getCollisions() {
    return collisions;
  }

  /** @return Total number of branches in the tree */
  public int getTotalBranches() {
    return total_branches;
  }

  /** @return Total number of leaves in the tree */
  public int getTotalLeaves() {
    return total_leaves;
  }

  /** @return When the tree was created, Unix epoch in seconds */
  public long getCreated() {
    return created;
  }

  /** @return Last time the tree was updated, Unix epoch in seconds */
  public long getLastUpdate() {
    return last_update;
  }

  /** @return Version of the tree */
  public long getVersion() {
    return version;
  }

  /** @return Tree node separator character */
  public char getNodeSeparator() {
    return node_separator;
  }

  /** @param name A descriptive name for the tree */
  public void setName(String name) {
    this.name = name;
  }

  /** @param description A brief description of the tree */
  public void setDescription(String description) {
    this.description = description;
  }

  /** @param notes Optional notes about the tree */
  public void setNotes(String notes) {
    this.notes = notes;
  }

  /** @param strict_match Whether or not a TSUID must match all rules in the
   * tree to be included */
  public void setStrictMatch(boolean strict_match) {
    this.strict_match = strict_match;
  }

  /** @param rules The tree's rule set */
  public void setRules(TreeMap<Integer, TreeMap<Integer, TreeRule>> rules) {
    this.rules = rules;
  }

  /** @param node_separator Node separation character */
  public void setNodeSeparator(char node_separator) {
    this.node_separator = node_separator;
  }
 
}

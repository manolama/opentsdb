package net.opentsdb.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a meta tree that organizes timeseries into a directory
 * like structure for easy navigation. Users can define a number of trees with
 * simple or complex sets of rules used to build the tree.
 */
public class Tree {
  private static final Logger LOG = LoggerFactory.getLogger(Tree.class);
  
  /** ID of the tree in storage */
  public int tree_id;
  
  /** Descriptive name of the tree provided by the user */
  public String name;
  
  /** Optional, detailed notes about the tree */
  public String notes;
  
  /** Whether or not to store non-matching branches/leaves in this tree */
  public boolean strict_match;
}

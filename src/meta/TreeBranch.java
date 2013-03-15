package net.opentsdb.meta;

/**
 * A tree branch represents a node of a meta tree and refers to it's parent and
 * what branches or leaves it contains.
 */
public class TreeBranch {
  
  /** The hash of the parent branch, may be 0 if it's the root */
  public int parent_hash;
  
  /** The depth of this branch in the tree */
  public int depth;
  
  /** The path of this branch */
  public String path;
  
  /** Name to display to the users of OpenTSDB */
  public String display_name;
  
  /** ID of the tree to which this branch belongs */
  public int tree_id;
  
  /** Version of the tree this branch belongs to, can be used for culling */
  public long tree_version;
}

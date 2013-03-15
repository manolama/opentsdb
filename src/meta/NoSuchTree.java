package net.opentsdb.meta;

import java.util.NoSuchElementException;

public class NoSuchTree extends NoSuchElementException{

  /** The tree ID requested */
  private final int tree_id;
  
  public NoSuchTree(final int tree_id) {
    this.tree_id = tree_id;
  }
  
  public int tree_id() {
    return this.tree_id;
  }
  
  private static final long serialVersionUID = 1476869263116427978L;
}

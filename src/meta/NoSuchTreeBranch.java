package net.opentsdb.meta;

import java.util.NoSuchElementException;

public class NoSuchTreeBranch extends NoSuchElementException {

  /** ID of the tree requested */
  private int tree_id;
  
  /** ID of the branch requested */
  private int branch_id;
  
  public NoSuchTreeBranch(final int tree_id, final int branch_id) {
    this.tree_id = tree_id;
    this.branch_id = branch_id;
  }
  
  public int tree_id() {
    return this.tree_id;
  }
  
  public int branch_id() {
    return this.branch_id;
  }
  
  private static final long serialVersionUID = 4691679581805065797L;
}

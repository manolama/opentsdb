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

import java.util.HashSet;


/**
 * MAY NOT USE THIS
 * Overloads the Tree class with setters usable by the TSD internally so that
 * we don't let users modify read-only variables
 * @since 2.0 
 */
final class InternalTree extends Tree {

  /** @param treeId ID of the tree, users cannot modify this */
  public void setTreeId(int treeId) {
    this.treeId = treeId;
  }

  /** @param not_matched List of TSUIDs that didn't match rules */
  public void setNotMatched(HashSet<String> not_matched) {
    this.not_matched = not_matched;
  }

  /** @param collisions List of TSUIDs not included due to collisions */
  public void setCollisions(HashSet<String> collisions) {
    this.collisions = collisions;
  }

  /** @param total_branches Total number of branches */
  public void setTotalBranches(int total_branches) {
    this.total_branches = total_branches;
  }

  /** @param total_leaves Total number of leaves */
  public void setTotalLeaves(int total_leaves) {
    this.total_leaves = total_leaves;
  }

  /** @param created The time when this tree was created, 
   * Unix epoch in seconds */
  public void setCreated(long created) {
    this.created = created;
  }

  /** @param last_update Last time the tree was modified, 
   * Unix epoch in seconds */
  public void setLastUpdate(long last_update) {
    this.last_update = last_update;
  }

  /** @param version Current version of the tree */
  public void setVersion(long version) {
    this.version = version;
  }

}

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

import net.opentsdb.tree.InternalTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.utils.JSON;

import org.junit.Test;

public final class TestTree {

  @Test
  public void ser() {
    System.out.println(JSON.serializeToString(new Tree()));
  }
  
  @Test
  public void des() {
    final String json = "{\"treeId\":0,\"name\":\"\",\"description\":\"UserDescription\",\"notes\":\"\",\"rules\":null,\"collisions\":null,\"created\":0,\"version\":0,\"strictMatch\":false,\"notMatched\":null,\"totalBranches\":69,\"totalLeaves\":24,\"lastUpdate\":42,\"nodeSeparator\":\"|\"}";
    Tree tree = JSON.parseToObject(json, InternalTree.class);
    System.out.println(JSON.serializeToString(tree));
  }
}

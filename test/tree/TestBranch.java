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
import java.util.TreeSet;

import net.opentsdb.utils.JSON;

import org.junit.Test;

public final class TestBranch {
  
  @Test
  public void ser() {
    Branch b = new Branch();    
    b.setTreeId(42);
    b.setDisplayName("Hello World");
    
    Branch ch = new Branch();
    ch.setTreeId(42);
    ch.setDisplayName("Child");
    
    TreeSet<Branch> branches = new TreeSet<Branch>();
    branches.add(ch);
    b.setBranches(branches);
    
    Leaf l = new Leaf();
    l.setTsuid("ABCD");
    l.setDisplayName("Super Leaf!");
    l.setDepth(4);
    TreeSet<Leaf> leaves = new TreeSet<Leaf>();
    leaves.add(l);
    b.setLeaves(leaves);
    
    System.out.println(new String(b.toJson(false), Charset.forName("UTF-8")));
  }
  
  @Test
  public void des() {
    final String json = "{\"treeId\":42,\"branchId\":1,\"parentId\":8,\"depth\":2,\"displayName\":\"Hello World\",\"numBranches\":1,\"numLeaves\":1,\"branches\":[{\"branchId\":0,\"displayName\":\"Child\",\"numBranches\":9,\"numLeaves\":7}],\"leaves\":[{\"depth\":4,\"displayName\":\"Super Leaf!\",\"tsuid\":\"ABCD\"}]}";
    Branch b = JSON.parseToObject(json, Branch.class);
    
    System.out.println(new String(b.toJson(false), Charset.forName("UTF-8")));
  }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public final class TestLeaf {

  @Test
  public void testEquals() {
    final Leaf leaf = new Leaf();
    leaf.setTsuid("ABCD");
    final Leaf leaf2 = new Leaf();
    leaf2.setTsuid("ABCD");
    assertTrue(leaf.equals(leaf2));
  }
  
  @Test
  public void equalsSameAddress() {
    final Leaf leaf = new Leaf();
    final Leaf leaf2 = leaf;
    assertTrue(leaf.equals(leaf2));
  }
  
  @Test
  public void equalsNull() {
    final Leaf leaf = new Leaf();
    assertFalse(leaf.equals(null));
  }
  
  @Test
  public void equalsWrongClass() {
    final Leaf leaf = new Leaf();
    assertFalse(leaf.equals(new Object()));
  }
  
  @Test
  public void compareTo() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Leaf");
    assertEquals(0, leaf.compareTo(leaf2));
  }
  
  @Test
  public void compareToLess() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Ardvark");
    assertTrue(leaf.compareTo(leaf2) > 0);
  }
  
  @Test
  public void compareToGreater() {
    final Leaf leaf = new Leaf();
    leaf.setDisplayName("Leaf");
    final Leaf leaf2 = new Leaf();
    leaf2.setDisplayName("Zelda");
    assertTrue(leaf.compareTo(leaf2) < 0);
  }
}

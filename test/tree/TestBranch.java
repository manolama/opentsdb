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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, HBaseClient.class, Scanner.class, PutRequest.class, 
  KeyValue.class})
public final class TestBranch {
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private Tree tree = TestTree.buildTestTree();
  private HashMap<String, HashMap<String, byte[]>> storage = 
    new HashMap<String, HashMap<String, byte[]>>();
  private int scanner_nexts = 0;
  final Scanner scanner = mock(Scanner.class);
  
  @Test
  public void testHashCode() {
    final Branch branch = buildTestBranch(tree);;
    assertEquals(2521314, branch.hashCode());
  }
  
  @Test
  public void testEquals() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    assertTrue(branch.equals(branch2));
  }
  
  @Test
  public void equalsSameAddress() {
    final Branch branch = buildTestBranch(tree);;
    assertTrue(branch.equals(branch));
  }
  
  @Test
  public void equalsNull() {
    final Branch branch = buildTestBranch(tree);;
    assertFalse(branch.equals(null));
  }
  
  @Test
  public void equalsWrongClass() {
    final Branch branch = buildTestBranch(tree);;
    assertFalse(branch.equals(new Object()));
  }
  
  @Test
  public void compareTo() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    assertEquals(0, branch.compareTo(branch2));
  }
  
  @Test
  public void compareToLess() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    branch2.setDisplayName("Ardvark");
    assertTrue(branch.compareTo(branch2) > 0);
  }
  
  @Test
  public void compareToGreater() {
    final Branch branch = buildTestBranch(tree);;
    final Branch branch2 = buildTestBranch(tree);;
    branch2.setDisplayName("Zelda");
    assertTrue(branch.compareTo(branch2) < 0);
  }
  
  @Test
  public void getBranchIdRoot() {
    final Branch branch = buildTestBranch(tree);;
    assertEquals("0001", branch.getBranchId());
  }
  
  @Test
  public void getBranchIdChild() {
    final Branch branch = buildTestBranch(tree);;
    assertEquals("0001D119F20E", branch.getBranches().first().getBranchId());
  }
  
  @Test
  public void addChild() throws Exception {
    final Branch branch = buildTestBranch(tree);
    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertEquals(3, branch.getNumBranches());
    assertEquals(2, branch.getNumLeaves());
  }
  
  @Test
  public void addChildNoLocalBranches() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    final Branch child = new Branch(tree.getTreeId());
    Field branches = Branch.class.getDeclaredField("branches");
    branches.setAccessible(true);
    branches.set(branch, null);
    branches.setAccessible(false);
    assertTrue(branch.addChild(child));
    assertEquals(1, branch.getNumBranches());
    assertEquals(2, branch.getNumLeaves());
  }
  
  @Test
  public void addChildNoChanges() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    final Branch child = new Branch(tree.getTreeId());
    assertTrue(branch.addChild(child));
    assertFalse(branch.addChild(child));
    assertEquals(3, branch.getNumBranches());
    assertEquals(2, branch.getNumLeaves());
  }
  
  @Test
  public void addLeafExists() throws Exception {
    final Tree tree = TestTree.buildTestTree();
    final Branch branch = buildTestBranch(tree);;

    Leaf leaf = new Leaf();
    leaf.setDisplayName("Alarms");
    leaf.setTsuid("ABCD");

    assertFalse(branch.addLeaf(leaf, tree));
    assertEquals(2, branch.getNumBranches());
    assertEquals(2, branch.getNumLeaves());
    assertNull(tree.getCollisions());
  }
  
  @Test
  public void addLeafCollision() throws Exception {
    final Tree tree = TestTree.buildTestTree();
    final Branch branch = buildTestBranch(tree);;

    Leaf leaf = new Leaf();
    leaf.setDisplayName("Alarms");
    leaf.setTsuid("0001");

    assertFalse(branch.addLeaf(leaf, tree));
    assertEquals(2, branch.getNumBranches());
    assertEquals(2, branch.getNumLeaves());
    assertEquals(1, tree.getCollisions().size());
  }

  @Test (expected = IllegalArgumentException.class)
  public void addChildNull() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    branch.addChild(null);
  }

  @Test
  public void addLeaf() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    
    Leaf leaf = new Leaf();
    leaf.setDisplayName("Application Servers");
    leaf.setTsuid("0004");
    
    assertTrue(branch.addLeaf(leaf, null));
  }

  @Test (expected = IllegalArgumentException.class)
  public void addLeafNull() throws Exception {
    final Branch branch = buildTestBranch(tree);;
    branch.addLeaf(null, null);
  }
  
  @Test
  public void compileBranchId() {
    final Branch branch = buildTestBranch(tree);;
    assertArrayEquals(new byte[] { 0, 1 }, branch.compileBranchId());
  }
  
  @Test
  public void compileBranchIdChild() {
    final Branch branch = buildTestBranch(tree);;
    assertArrayEquals(new byte[] { 0, 1 , (byte) 0xD1, 0x19, (byte) 0xF2, 0x0E }, 
        branch.getBranches().first().compileBranchId());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void compileBranchIdEmptyDisplayName() {
    final Branch branch = new Branch(1);
    branch.compileBranchId();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void compileBranchIdInvalidId() {
    final Branch branch = new Branch(0);
    branch.compileBranchId();
  }

  @Test
  public void fetchBranch() throws Exception {
    setupStorage();
    final Branch branch = Branch.fetchBranch(tsdb, 
        Branch.stringToId("00010001BECD000181A8"), false);
    System.out.println(JSON.serializeToString(branch));
    assertNotNull(branch);
    assertEquals(1, branch.getTreeId());
    assertEquals("cpu", branch.getDisplayName());
    assertEquals("00010001BECD000181A8", branch.getBranchId());
    assertEquals(1, branch.getBranches().size());
    assertEquals(2, branch.getLeaves().size());
  }
  
  @Test
  public void fetchBranchNotFound() throws Exception {
    setupStorage();
    when(scanner.nextRows()).thenAnswer(
        new Answer<Deferred<ArrayList<ArrayList<KeyValue>>>> () {
          @Override
          public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
              InvocationOnMock arg0) throws Throwable {
            return Deferred.fromResult(null);
        }});
    final Branch branch = Branch.fetchBranch(tsdb, 
        Branch.stringToId("00010001BECD000181A8"), false);
    assertNull(branch);
  }

  @Test
  public void storeBranch() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);;
    branch.storeBranch(tsdb, true);
    assertEquals(1, storage.size());
    assertEquals(3, storage.get("0001").size());
    final Branch parsed = JSON.parseToObject(storage.get("0001").get("branch"), 
        Branch.class);
    parsed.setTreeId(1);
    assertEquals("ROOT", parsed.getDisplayName());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchMissingTreeID() throws Exception {
    setupStorage();
    final Branch branch = new Branch();
    branch.storeBranch(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID0() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);;
    branch.setTreeId(0);
    branch.storeBranch(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeBranchTreeID65536() throws Exception {
    setupStorage();
    final Branch branch = buildTestBranch(tree);;
    branch.setTreeId(65536);
    branch.storeBranch(tsdb, false);
  }

  /**
   * Helper to build a default branch for testing
   * @return A branch with some child branches and leaves
   */
  public static Branch buildTestBranch(final Tree tree) {
    final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    final Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    
    Branch child = new Branch(1);
    child.prependParentPath(root_path);
    child.setDisplayName("System");
    root.addChild(child);
    
    child = new Branch(tree.getTreeId());
    child.prependParentPath(root_path);
    child.setDisplayName("Network");
    root.addChild(child);

    Leaf leaf = new Leaf();
    leaf.setDisplayName("Alarms");
    leaf.setTsuid("ABCD");
    root.addLeaf(leaf, tree);
    
    leaf = new Leaf();
    leaf.setDisplayName("Employees in Office");
    leaf.setTsuid("EF00");
    root.addLeaf(leaf, tree);

    return root;
  }
  
  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage() {
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());

    Charset ascii = Charset.forName("ISO-8859-1");
    Charset utf = Charset.forName("UTF-8");
    final byte[] LEAF_PREFIX = "leaf:".getBytes(ascii);
    
    // main branch
    byte[] main_row = Branch.stringToId("00010001BECD000181A8");
    ArrayList<KeyValue> main = new ArrayList<KeyValue>();
    KeyValue branch = mock(KeyValue.class);
    main.add(branch);
    when(branch.qualifier()).thenReturn("branch".getBytes(ascii));
    when(branch.value()).thenReturn(
        ("{\"path\":{\"0\":\"ROOT\",\"1\":\"sys\",\"2\":\"cpu\"},\"displayName" + 
        "\":\"cpu\"}").getBytes(utf));
    when(branch.key()).thenReturn(main_row);
    
    KeyValue leaf1 = mock(KeyValue.class);
    main.add(leaf1);
    final byte[] leaf1q = new byte[5 + 9];
    System.arraycopy(LEAF_PREFIX, 0, leaf1q, 0, LEAF_PREFIX.length);
    System.arraycopy(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1}, 0, leaf1q, 5, 9);
    when(leaf1.qualifier()).thenReturn(leaf1q);
    when(leaf1.value()).thenReturn("{\"displayName\":\"user\"}".getBytes(utf));
    when(leaf1.key()).thenReturn(main_row);
    
    KeyValue leaf2 = mock(KeyValue.class);
    main.add(leaf2);
    final byte[] leaf2q = new byte[5 + 9];
    System.arraycopy(LEAF_PREFIX, 0, leaf2q, 0, LEAF_PREFIX.length);
    System.arraycopy(new byte[] { 0, 0, 2, 0, 0, 2, 0, 0, 2}, 0, leaf2q, 5, 9);
    when(leaf2.qualifier()).thenReturn(leaf2q);
    when(leaf2.value()).thenReturn("{\"displayName\":\"nice\"}".getBytes(utf));
    when(leaf2.key()).thenReturn(main_row);
    
    // child branch
    byte[] child_row = Branch.stringToId("00010001BECD000181A8000190AC");
    ArrayList<KeyValue> child = new ArrayList<KeyValue>();
    
    KeyValue branch2 = mock(KeyValue.class);
    child.add(branch2);
    when(branch2.qualifier()).thenReturn("branch".getBytes(ascii));
    when(branch2.value()).thenReturn(
        ("{\"path\":{\"0\":\"ROOT\",\"1\":\"sys\",\"2\":\"cpu\",\"3\":\"gpu\"}" + 
        ",\"displayName\":\"gpu\"}").getBytes(utf));
    when(branch2.key()).thenReturn(child_row);
    
    KeyValue leaf3 = mock(KeyValue.class);
    child.add(leaf3);
    final byte[] leaf3q = new byte[5 + 9];
    System.arraycopy(LEAF_PREFIX, 0, leaf3q, 0, LEAF_PREFIX.length);
    System.arraycopy(new byte[] { 0, 0, 3, 0, 0, 3, 0, 0, 3}, 0, leaf3q, 5, 9);
    when(leaf3.qualifier()).thenReturn(leaf3q);
    when(leaf3.value()).thenReturn("{\"displayName\":\"nvidia\"}".getBytes(utf));
    when(leaf3.key()).thenReturn(child_row);
    
    final ArrayList<ArrayList<KeyValue>> rows = 
      new ArrayList<ArrayList<KeyValue>>();
    rows.add(main);
    rows.add(child);
    
    when(client.newScanner((byte[]) any())).thenReturn(scanner);
   
    when(scanner.nextRows()).thenAnswer(
        new Answer<Deferred<ArrayList<ArrayList<KeyValue>>>>() {

          @Override
          public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
              final InvocationOnMock invocation) throws Throwable {
            if (scanner_nexts > 0) {
              return Deferred.fromResult(null);
            }
            
            scanner_nexts++;
            return Deferred.fromResult(rows);
          }
          
        });
    
    when (client.compareAndSet((PutRequest)any(), (byte[])any()))
      .thenAnswer(new Answer<Deferred<Boolean>>() {

        @Override
        public Deferred<Boolean> answer(final InvocationOnMock invocation) 
          throws Throwable {
          final Object[] args = invocation.getArguments();
          final PutRequest put = (PutRequest)args[0];
          final String key = Branch.idToString(put.key());
          HashMap<String, byte[]> column = storage.get(key);
          if (column == null) {
            column = new HashMap<String, byte[]>();
            storage.put(key, column);
          }
          column.put(new String(put.qualifier()), put.value());
          return Deferred.fromResult(true);
        }
        
      });
  }
}

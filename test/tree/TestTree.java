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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
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
@PrepareForTest({TSDB.class, HBaseClient.class, GetRequest.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class})
public final class TestTree {
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private HashMap<String, HashMap<String, byte[]>> storage = 
    new HashMap<String, HashMap<String, byte[]>>();
  private int scanner_nexts = 0;
  final Scanner scanner = mock(Scanner.class);
  
  @Test
  public void copyChanges() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = buildTestTree();
    tree2.setName("Different Tree");
    assertTrue(tree.copyChanges(tree2, false));
    assertEquals("Different Tree", tree.getName());
  }
  
  @Test
  public void copyChangesNone() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = buildTestTree();
    assertFalse(tree.copyChanges(tree2, false));
  }
  
  @Test
  public void copyChangesOverride() throws Exception {
    final Tree tree = buildTestTree();
    final Tree tree2 = new Tree(1);
    assertTrue(tree.copyChanges(tree2, true));
    assertTrue(tree.getName().isEmpty());
    assertTrue(tree.getDescription().isEmpty());
    assertTrue(tree.getNotes().isEmpty());
  }

  @Test
  public void serialize() throws Exception {
    final String json = JSON.serializeToString(buildTestTree());
    assertNotNull(json);
    System.out.println(json);
    assertTrue(json.contains("\"created\":1356998400"));
    assertTrue(json.contains("\"name\":\"Test Tree\""));
    assertTrue(json.contains("\"description\":\"My Description\""));
  }
  
  @Test
  public void deserialize() throws Exception {
    Tree tree = JSON.parseToObject(getSerializedTree(), Tree.class);
    assertNotNull(tree);
    assertEquals("Test Tree", tree.getName());
    assertEquals(5, tree.getRules().size());
  }

  @Test
  public void addRule() throws Exception {
    final Tree tree = new Tree();
    tree.addRule(new TreeRule());
    assertNotNull(tree.getRules());
    assertEquals(1, tree.getRules().size());
  }
  
  @Test
  public void addRuleLevel() throws Exception {
    final Tree tree = new Tree();
    TreeRule rule = new TreeRule(1);
    rule.setDescription("MyRule");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    assertNotNull(tree.getRules());
    assertEquals(1, tree.getRules().size());
    assertEquals("MyRule", tree.getRules().get(1).get(1).getDescription());
    
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRuleNull() throws Exception {
    final Tree tree = new Tree();
    tree.addRule(null);
  }

  @Test
  public void addCollision() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision("010203", "AABBCCDD");
    assertEquals(1, tree.getCollisions().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addCollisionNull() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision(null, "AABBCCDD");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addCollisionEmpty() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getCollisions());
    tree.addCollision("", "AABBCCDD");
  }
  
  @Test
  public void addNoMatch() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched("010203", "Bummer");
    assertEquals(1, tree.getNotMatched().size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addNoMatchNull() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched(null, "Bummer");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addNoMatchEmpty() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getNotMatched());
    tree.addNotMatched("", "Bummer");
  }
  
  @Test
  public void storeTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setName("New Name");
    assertTrue(tree.storeTree(tsdb, false).joinUninterruptibly());
  }
  
  @Test (expected = IllegalStateException.class)
  public void storeTreeNoChanges() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.storeTree(tsdb, false);
    tree.storeTree(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeTreeTreeID0() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setTreeId(0);
    tree.storeTree(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeTreeTreeID655536() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.setTreeId(655536);
    tree.storeTree(tsdb, false);
  }
  
  @Test
  public void storeTreeWCollisions() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addCollision("010203", "AABBCCDD");
    assertTrue(tree.storeTree(tsdb, false).joinUninterruptibly());
    assertEquals(4, storage.size());
    assertEquals(3, storage.get("000101").size());
  }
  
  @Test
  public void storeTreeWCollisionExisting() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addCollision("010101", "AAAAAA");
    assertTrue(tree.storeTree(tsdb, false).joinUninterruptibly());
    assertEquals(4, storage.size());
    assertEquals(2, storage.get("000101").size());
  }
  
  @Test
  public void storeTreeWNotMatched() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addNotMatched("010203", "Failed rule 2:2");
    assertTrue(tree.storeTree(tsdb, false).joinUninterruptibly());
    assertEquals(4, storage.size());
    assertEquals(3, storage.get("000102").size());
  }
  
  @Test
  public void storeTreeWNotMatchedExisting() throws Exception {
    setupStorage(true, true);
    final Tree tree = buildTestTree();
    tree.addNotMatched("010101", "Failed rule 4:4");
    assertTrue(tree.storeTree(tsdb, false).joinUninterruptibly());
    assertEquals(4, storage.size());
    assertEquals(2, storage.get("000102").size());
  }
  
  @Test
  public void getRule() throws Exception {
    final TreeRule rule = buildTestTree().getRule(3, 0);
    assertNotNull(rule);
    assertEquals(TreeRuleType.METRIC, rule.getType());
  }
  
  @Test
  public void getRuleNullSet() throws Exception {
    final Tree tree = buildTestTree();
    Field rules = Tree.class.getDeclaredField("rules");
    rules.setAccessible(true);
    rules.set(tree, null);
    rules.setAccessible(false);
    assertNull(tree.getRule(3, 0));
  }
  
  @Test
  public void getRuleNoLevel() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getRule(42, 0));
  }
  
  @Test
  public void getRuleNoOrder() throws Exception {
    final Tree tree = buildTestTree();
    assertNull(tree.getRule(3, 42));
  }

  @Test
  public void createNewTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tree.createNewTree(tsdb);
    assertEquals(3, tree_id);
    assertEquals(5, storage.size());
    assertEquals(1, storage.get("0003").size());
  }

  @Test
  public void createNewFirstTree() throws Exception {
    setupStorage(true, true);
    storage.clear();
    final Tree tree = new Tree();
    tree.setName("New Tree");
    final int tree_id = tree.createNewTree(tsdb);
    assertEquals(1, tree_id);
    assertEquals(1, storage.size());
    assertEquals(1, storage.get("0001").size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void createNewTreeNoChanges() throws Exception {
    setupStorage(true, true);
    final Tree tree = new Tree();
    tree.createNewTree(tsdb);
  }
  
  @Test (expected = IllegalStateException.class)
  public void createNewTreeOutOfIDs() throws Exception {
    setupStorage(true, true);

    final Tree max_tree = new Tree(65535);
    max_tree.setName("max");
    HashMap<String, byte[]> col = new HashMap<String, byte[]>();
    storage.put("FFFF", col);
    col.put("tree", JSON.serializeToBytes(max_tree));
    
    final Tree tree = new Tree();
    tree.createNewTree(tsdb);
  }

  @Test
  public void fetchTree() throws Exception {
    setupStorage(true, true);
    final Tree tree = Tree.fetchTree(tsdb, 1);
    assertNotNull(tree);
    assertEquals("Test Tree", tree.getName());
    assertEquals(2, tree.getRules().size());
  }
  
  @Test
  public void fetchTreeDoesNotExist() throws Exception {
    setupStorage(true, true);
    assertNull(Tree.fetchTree(tsdb, 3));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchTreeID0() throws Exception {
    Tree.fetchTree(tsdb, 0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchTreeID65536() throws Exception {
    Tree.fetchTree(tsdb, 65536);
  }
  
  @Test
  public void fetchAllTrees() throws Exception {
    setupStorage(true, true);
    final List<Tree> trees = Tree.fetchAllTrees(tsdb);
    assertNotNull(trees);
    assertEquals(2, trees.size());
    for (Tree tree : trees) {
      System.out.println("Returned tree ID: " + tree.getTreeId());
    }
  }
  
  @Test
  public void fetchAllTreesNone() throws Exception {
    setupStorage(true, true);
    storage.clear();
    final List<Tree> trees = Tree.fetchAllTrees(tsdb);
    assertNotNull(trees);
    assertEquals(0, trees.size());
  }
  
  @Test
  public void treeExists() throws Exception {
    setupStorage(true, true);
    assertTrue(Tree.treeExists(tsdb, 1));
  }
  
  @Test
  public void treeExists2() throws Exception {
    setupStorage(true, true);
    assertTrue(Tree.treeExists(tsdb, 2));
  }

  @Test
  public void treeExistsNot() throws Exception {
    setupStorage(true, true);
    assertFalse(Tree.treeExists(tsdb, 3));
  }
  
  @Test
  public void fetchAllCollisions() throws Exception {
    setupStorage(true, true);
    Map<String, String> collisions = Tree.fetchCollisions(tsdb, 1, null);
    assertNotNull(collisions);
    assertEquals(2, collisions.size());
    assertTrue(collisions.containsKey("010101"));
    assertTrue(collisions.containsKey("020202"));
  }
  
  @Test
  public void fetchAllCollisionsNone() throws Exception {
    setupStorage(true, true);
    storage.remove("000101");
    Map<String, String> collisions = Tree.fetchCollisions(tsdb, 1, null);
    assertNotNull(collisions);
    assertEquals(0, collisions.size());
  }
  
  @Test
  public void fetchCollisionsSingle() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> collisions = Tree.fetchCollisions(tsdb, 1, tsuids);
    assertNotNull(collisions);
    assertEquals(1, collisions.size());
    assertTrue(collisions.containsKey("020202"));
  }
  
  @Test
  public void fetchCollisionsSingleNotFound() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> collisions = Tree.fetchCollisions(tsdb, 1, tsuids);
    assertNotNull(collisions);
    assertEquals(0, collisions.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID0() throws Exception {    
    Tree.fetchCollisions(tsdb, 0, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchCollisionsID655536() throws Exception {    
    Tree.fetchCollisions(tsdb, 655536, null);
  }
  
  @Test
  public void fetchAllNotMatched() throws Exception {
    setupStorage(true, true);
    Map<String, String> not_matched = Tree.fetchNotMatched(tsdb, 1, null);
    assertNotNull(not_matched);
    assertEquals(2, not_matched.size());
    assertTrue(not_matched.containsKey("010101"));
    assertEquals("Failed rule 0:0", not_matched.get("010101"));
    assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));
  }
  
  @Test
  public void fetchAllNotMatchedNone() throws Exception {
    setupStorage(true, true);
    storage.remove("000102");
    Map<String, String> not_matched = Tree.fetchNotMatched(tsdb, 1, null);
    assertNotNull(not_matched);
    assertEquals(0, not_matched.size());
  }
  
  @Test
  public void fetchNotMatchedSingle() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("020202");
    Map<String, String> not_matched = Tree.fetchNotMatched(tsdb, 1, tsuids);
    assertNotNull(not_matched);
    assertEquals(1, not_matched.size());
    assertTrue(not_matched.containsKey("020202"));
    assertEquals("Failed rule 1:1", not_matched.get("020202"));
  }
  
  @Test
  public void fetchNotMatchedSingleNotFound() throws Exception {
    setupStorage(true, true);
    final ArrayList<String> tsuids = new ArrayList<String>(1);
    tsuids.add("030303");
    Map<String, String> not_matched = Tree.fetchNotMatched(tsdb, 1, tsuids);
    assertNotNull(not_matched);
    assertEquals(0, not_matched.size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID0() throws Exception {    
    Tree.fetchNotMatched(tsdb, 0, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchNotMatchedID655536() throws Exception {    
    Tree.fetchNotMatched(tsdb, 655536, null);
  }
  
  @Test
  public void deleteTree() throws Exception {
    setupStorage(true, true);
    assertNotNull(Tree.deleteTree(tsdb, 1).joinUninterruptibly());
    assertTrue(storage.isEmpty());
  }
  
  @Test
  public void idToBytes() throws Exception {
    assertArrayEquals(new byte[]{ 0, 1 }, Tree.idToBytes(1));
  }
  
  @Test
  public void idToBytesMax() throws Exception {
    assertArrayEquals(new byte[]{ (byte) 0xFF, (byte) 0xFF }, 
        Tree.idToBytes(65535));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void idToBytesBadID0() throws Exception {
    Tree.idToBytes(0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void idToBytesBadID655536() throws Exception {
    Tree.idToBytes(655536);
  }
  
  @Test
  public void bytesToId() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1 }));
  }
  
  @Test
  public void bytesToIdMetaRow() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1, 1 }));
  }

  @Test
  public void bytesToIdBranchRow() throws Exception {
    assertEquals(1, Tree.bytesToId(new byte[] { 0, 1, 4, 2, 1, 0 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void bytesToIdBadRow() throws Exception {
    Tree.bytesToId(new byte[] { 1 });
  }
  
  /**
   * Returns a 5 level rule set that parses a data center, a service, the 
   * hostname, metric and some tags from meta data.
   * @param tree The tree to add the rules to
   */
  public static void buildTestRuleSet(final Tree tree) {

    // level 0
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Datacenter");
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
    rule.setField("host");
    rule.setDescription("Datacenter");
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 1
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("fqdn");
    rule.setDescription("Service");
    rule.setLevel(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
    rule.setField("host");
    rule.setDescription("Service");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 2
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("fqdn");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("host");
    rule.setDescription("Hostname");
    rule.setLevel(2);
    rule.setOrder(1);
    tree.addRule(rule);
    
    // level 3
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC);
    rule.setDescription("Metric split");
    rule.setSeparator("\\.");
    rule.setLevel(3);
    tree.addRule(rule);
    
    // level 4
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("type");
    rule.setDescription("Type Tag");
    rule.setLevel(4);
    rule.setOrder(0);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("method");
    rule.setDescription("Method Tag");
    rule.setLevel(4);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK);
    rule.setField("port");
    rule.setDescription("Port Tag");
    rule.setDisplayFormat("Port: {value}");
    rule.setLevel(4);
    rule.setOrder(2);
    tree.addRule(rule);
  }
  
  /**
   * Returns a configured tree with rules and values for testing purposes
   * @return A tree to test with
   */
  public static Tree buildTestTree() {
    final Tree tree = new Tree();
    tree.setTreeId(1);
    tree.setCreated(1356998400L);
    tree.setDescription("My Description");
    tree.setName("Test Tree");
    tree.setNotes("Details");   
    buildTestRuleSet(tree);
    
    // reset the changed field via reflection
    Method reset;
    try {
      reset = Tree.class.getDeclaredMethod("initializeChangedMap");
      reset.setAccessible(true);
      reset.invoke(tree);
      reset.setAccessible(false);
    // Since some other tests are calling this as a constructor, we can't throw
    // exceptions. So just print them.
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return tree;
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage(final boolean default_get, 
      final boolean default_put) throws Exception {
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    when(client.newScanner((byte[]) any())).thenReturn(scanner);
    
    final Charset ascii = Charset.forName("ISO-8859-1");
    
    // set existing storage values
    HashMap<String, byte[]> col = new HashMap<String, byte[]>();
    storage.put("0001", col);
    
    col.put("tree", JSON.serializeToBytes(buildTestTree()));
    
    TreeRule rule = new TreeRule(1);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    col.put("tree_rule:0:0", JSON.serializeToBytes(rule));
    new TreeRule(1);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    col.put("tree_rule:1:0", JSON.serializeToBytes(rule));
    Branch root = new Branch(1);
    root.setDisplayName("ROOT");
    TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    Method branch_json = Branch.class.getDeclaredMethod("toStorageJson");
    branch_json.setAccessible(true);
    col.put("branch", (byte[])branch_json.invoke(root));
    
    // tree 2
    col = new HashMap<String, byte[]>();
    storage.put("0002", col);
    
    Tree tree2 = new Tree();
    tree2.setTreeId(2);
    tree2.setName("2nd Tree");
    tree2.setDescription("Other Tree");
    col.put("tree", JSON.serializeToBytes(tree2));
    
    rule = new TreeRule(2);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    col.put("tree_rule:0:0", JSON.serializeToBytes(rule));
    new TreeRule(2);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    col.put("tree_rule:1:0", JSON.serializeToBytes(rule));
    root = new Branch(2);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    col.put("branch", (byte[])branch_json.invoke(root));
    branch_json.setAccessible(false);
    
    // sprinkle in some collisions and no matches for fun
    // collisions
    col = new HashMap<String, byte[]>();
    storage.put("000101", col);
    String tsuid = "010101";
    byte[] qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    col.put(new String(qualifier, ascii), "AAAAAA".getBytes(ascii));
    
    tsuid = "020202";
    qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    col.put(new String(qualifier, ascii), "BBBBBB".getBytes(ascii));
    
    // not matched
    col = new HashMap<String, byte[]>();
    storage.put("000102", col);
    tsuid = "010101";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    col.put(new String(qualifier, ascii), "Failed rule 0:0".getBytes(ascii));
    
    tsuid = "020202";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    col.put(new String(qualifier, ascii), "Failed rule 1:1".getBytes(ascii));
    
    // setup storage access
    if (default_get) {
      when(client.get((GetRequest)any())).thenAnswer(
        new Answer<Deferred<ArrayList<KeyValue>>>() {

        @Override
        public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
            throws Throwable {
          final Object[] args = invocation.getArguments();
          final GetRequest get = (GetRequest)args[0];
          final String key = Branch.idToString(get.key());
          final HashMap<String, byte[]> row = storage.get(key);
          if (row == null) {
            return Deferred.fromResult((ArrayList<KeyValue>)null);
          } if (get.qualifiers() == null || get.qualifiers().length == 0) { 
            // return all
            ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
            for (Map.Entry<String, byte[]> entry : row.entrySet()) {
              KeyValue kv = mock(KeyValue.class);
              when(kv.value()).thenReturn(entry.getValue());
              when(kv.qualifier()).thenReturn(entry.getKey().getBytes(ascii));
              kvs.add(kv);
            }
            return Deferred.fromResult(kvs);
          } else {
            final String qualifier = new String(get.qualifiers()[0], ascii);
            if (!row.containsKey(qualifier)) {
              return Deferred.fromResult((ArrayList<KeyValue>)null);
            }
            
            ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
            KeyValue kv = mock(KeyValue.class);
            when(kv.value()).thenReturn(row.get(qualifier));
            when(kv.qualifier()).thenReturn(qualifier.getBytes(ascii));
            kvs.add(kv);
            return Deferred.fromResult(kvs);
          }
        }
        
      });
      
      when(scanner.nextRows()).thenAnswer(
        new Answer<Deferred<ArrayList<ArrayList<KeyValue>>>>() {

          @Override
          public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
              final InvocationOnMock invocation) throws Throwable {
            if (scanner_nexts > 0) {
              return Deferred.fromResult(null);
            }
            
            // return all
            ArrayList<ArrayList<KeyValue>> results = 
              new ArrayList<ArrayList<KeyValue>>(2);
            for (Map.Entry<String, HashMap<String, byte[]>> row : storage.entrySet()) {
              ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.getValue().size());
              System.out.println("Returning row: " + row.getKey());
              for (Map.Entry<String, byte[]> entry : row.getValue().entrySet()) {
                KeyValue kv = mock(KeyValue.class);
                when(kv.key()).thenReturn(Branch.stringToId(row.getKey()));
                when(kv.value()).thenReturn(entry.getValue());
                when(kv.qualifier()).thenReturn(entry.getKey().getBytes(ascii));
                kvs.add(kv);
              }
              results.add(kvs);
            }
            scanner_nexts++;
            return Deferred.fromResult(results);
          }
          
        });
    }
    
    if (default_put) {
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
    
    when(client.delete((DeleteRequest)any())).thenAnswer(
        new Answer<Deferred<Object>>() {

        @Override
        public Deferred<Object> answer(InvocationOnMock invocation)
            throws Throwable {
          final Object[] args = invocation.getArguments();
          final DeleteRequest delete = (DeleteRequest)args[0];
          final String key = Branch.idToString(delete.key());
          
          if (!storage.containsKey(key)) {
            return Deferred.fromResult(null);
          }
          
          // if no qualifiers, then delete the row
          if (delete.qualifiers() == null) {
            storage.remove(key);
            System.out.println("Told to delete row: " + key);
            return Deferred.fromResult(new Object());
          }
          
          HashMap<String, byte[]> column = storage.get(key);
          final byte[][] qualfiers = delete.qualifiers();       
          
          for (byte[] qualifier : qualfiers) {
            final String q = new String(qualifier, ascii);
            if (!column.containsKey(q)) {
              continue;
            }
            System.out.println("Told to delete column: " + q);
            column.remove(q);
          }
          
          // if all columns were deleted, wipe the row
          if (column.isEmpty()) {
            storage.remove(key);
            System.out.println("Deleting empty row: " + key);
          }
          return Deferred.fromResult(new Object());
        }
      
    });
        
  }

  /**
   * Method to store a massive, messy serialized tree with a bunch of rules and
   * information
   * @return A serialized tree
   */
  private String getSerializedTree() {
    return "{\"name\":\"Test Tree\",\"description\":\"My " +
    "Description\",\"notes\":\"Details\",\"rules\":{\"0\":{\"0\":{\"type\":" +
    "\"TAGK\",\"field\":\"fqdn\",\"regex\":\"^.*\\\\.([a-zA-Z]{3,4})[0-9]{0,1}" + 
    "\\\\..*\\\\..*$\",\"separator\":\"\",\"description\":\"Datacenter\",\"notes" + 
    "\":\"\",\"level\":0,\"order\":0,\"treeId\":1,\"customField\":\"\"," + 
    "\"regexGroupIdx\":0,\"displayFormat\":\"\"},\"1\":{\"type\":\"TAGK\"," +
    "\"field\":\"host\",\"regex\":\"^.*\\\\.([a-zA-Z]{3,4})[0-9]{0,1}\\\\..*\\" + 
    "\\..*$\",\"separator\":\"\",\"description\":\"Datacenter\",\"notes\":\"\"," +
    "\"level\":0,\"order\":1,\"treeId\":1,\"customField\":\"\",\"regexGroupIdx" +
    "\":0,\"displayFormat\":\"\"}},\"1\":{\"0\":{\"type\":\"TAGK\",\"field\":" +
    "\"fqdn\",\"regex\":\"^([a-zA-Z]+)(\\\\-|[0-9])*.*\\\\..*$\",\"separator\":" +
    "\"\",\"description\":\"Service\",\"notes\":\"\",\"level\":1,\"order\":0," +
    "\"treeId\":1,\"customField\":\"\",\"regexGroupIdx\":0,\"displayFormat\":" +
    "\"\"},\"1\":{\"type\":\"TAGK\",\"field\":\"host\",\"regex\":\"^" +
    "([a-zA-Z]+)(\\\\-|[0-9])*.*\\\\..*$\",\"separator\":\"\",\"description\":" +
    "\"Service\",\"notes\":\"\",\"level\":1,\"order\":1,\"treeId\":1," +
    "\"customField\":\"\",\"regexGroupIdx\":0,\"displayFormat\":\"\"}},\"2\"" +
    ":{\"0\":{\"type\":\"TAGK\",\"field\":\"fqdn\",\"regex\":\"\",\"separator" +
    "\":\"\",\"description\":\"Hostname\",\"notes\":\"\",\"level\":2,\"order" +
    "\":0,\"treeId\":1,\"customField\":\"\",\"regexGroupIdx\":0,\"" +
    "displayFormat\":\"\"},\"1\":{\"type\":\"TAGK\",\"field\":\"host\"," +
    "\"regex\":\"\",\"separator\":\"\",\"description\":\"Hostname\",\"notes\":" +
    "\"\",\"level\":2,\"order\":1,\"treeId\":1,\"customField\":\"\",\"" +
    "regexGroupIdx\":0,\"displayFormat\":\"\"}},\"3\":{\"0\":{\"type\":" +
    "\"METRIC\",\"field\":\"\",\"regex\":\"\",\"separator\":\"\"," +
    "\"description\":\"Metric split\",\"notes\":\"\",\"level\":3,\"order\":0," +
    "\"treeId\":1,\"customField\":\"\",\"regexGroupIdx\":0,\"displayFormat" +
    "\":\"\"}},\"4\":{\"0\":{\"type\":\"TAGK\",\"field\":\"type\",\"regex\":" +
    "\"\",\"separator\":\"\",\"description\":\"Type Tag\",\"notes\":\"\"," +
    "\"level\":4,\"order\":0,\"treeId\":1,\"customField\":\"\",\"" +
    "regexGroupIdx\":0,\"displayFormat\":\"\"},\"1\":{\"type\":\"TAGK\"," +
    "\"field\":\"method\",\"regex\":\"\",\"separator\":\"\",\"description\"" +
    ":\"Method Tag\",\"notes\":\"\",\"level\":4,\"order\":1,\"treeId\":1," +
    "\"customField\":\"\",\"regexGroupIdx\":0,\"displayFormat\":\"\"},\"2\"" +
    ":{\"type\":\"TAGK\",\"field\":\"port\",\"regex\":\"\",\"separator\":\"\"" +
    ",\"description\":\"Port Tag\",\"notes\":\"\",\"level\":4,\"order\":2," +
    "\"treeId\":1,\"customField\":\"\",\"regexGroupIdx\":0,\"displayFormat\"" +
    ":\"\"}}},\"collisions\":[\"0001\",\"0002\"],\"created\":1356998400," +
    "\"treeId\":1,\"lastUpdate\":1356998400,\"notMatched\":" +
    "[\"EF01\",\"ABCD\"],\"strictMatch\":false,\"nodeSeparator\":\"|\"}";
  }
}

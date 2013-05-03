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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.TreeMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.TestTree;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSDB.class, HBaseClient.class, GetRequest.class, Tree.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class })
public final class TestTreeRpc {
  private TSDB tsdb;
  private MockBase storage;
  private TreeRpc rpc = new TreeRpc();
  private Tree tree = TestTree.buildTestTree();
  // for UTs we'll use 1 byte tag IDs
  private String tsuid = "0102030405";
  private TSMeta meta = new TSMeta(tsuid);
  private UIDMeta metric = new UIDMeta(UniqueIdType.METRIC, new byte[] { 1 }, 
      "sys.cpu.0");
  private UIDMeta tagk1 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 2 }, 
      "host");
  private UIDMeta tagv1 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 3 }, 
      "web-01.lga.mysite.com");
  private UIDMeta tagk2 = new UIDMeta(UniqueIdType.TAGK, new byte[] { 4 }, 
      "type");
  private UIDMeta tagv2 = new UIDMeta(UniqueIdType.TAGV, new byte[] { 5 }, 
      "user");
  private byte[] leaf_qualifier;
  
  final static private Method toStorageJson;
  static {
    try {
      toStorageJson = Branch.class.getDeclaredMethod("toStorageJson");
      toStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  final static private Method TreetoStorageJson;
  static {
    try {
      TreetoStorageJson = Tree.class.getDeclaredMethod("toStorageJson");
      TreetoStorageJson.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    storage = new MockBase(true, true, true, true);
    tsdb = NettyMocks.getMockedHTTPTSDB(storage.getTSDB());
    
    // set private fields via reflection so the UTs can change things at will
    Field tag_metric = TSMeta.class.getDeclaredField("metric");
    tag_metric.setAccessible(true);
    tag_metric.set(meta, metric);
    tag_metric.setAccessible(false);
    
    ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(4);
    tags.add(tagk1);
    tags.add(tagv1);
    tags.add(tagk2);
    tags.add(tagv2);
    Field tags_field = TSMeta.class.getDeclaredField("tags");
    tags_field.setAccessible(true);
    tags_field.set(meta, tags);
    tags_field.setAccessible(false);

    leaf_qualifier = new byte[Leaf.LEAF_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Leaf.LEAF_PREFIX(), 0, leaf_qualifier, 0, 
        Leaf.LEAF_PREFIX().length);
    byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, leaf_qualifier, Leaf.LEAF_PREFIX().length, 
        tsuid_bytes.length);

    // store root
    TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
    Branch root = new Branch(tree.getTreeId());
    root.setDisplayName("ROOT");
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(root.compileBranchId(), 
        "branch".getBytes(MockBase.ASCII()), 
        (byte[])toStorageJson.invoke(root));
    
    byte[] key = new byte[] { 0, 1 };
    // set pre-test values
    storage.addColumn(key, "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(TestTree.buildTestTree()));
    
    TreeRule rule = new TreeRule(1);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    storage.addColumn(key, "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));

    rule = new TreeRule(1);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    root = new Branch(1);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    // TODO - static
    Method branch_json = Branch.class.getDeclaredMethod("toStorageJson");
    branch_json.setAccessible(true);
    storage.addColumn(key, "branch".getBytes(MockBase.ASCII()), 
        (byte[])branch_json.invoke(root));
    
    // tree 2
    key = new byte[] { 0, 2 };

    Tree tree2 = new Tree();
    tree2.setTreeId(2);
    tree2.setName("2nd Tree");
    tree2.setDescription("Other Tree");
    storage.addColumn(key, "tree".getBytes(MockBase.ASCII()), 
        (byte[])TreetoStorageJson.invoke(tree2));
    
    rule = new TreeRule(2);
    rule.setField("host");
    rule.setType(TreeRuleType.TAGK);
    storage.addColumn(key, "tree_rule:0:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    rule = new TreeRule(2);
    rule.setField("");
    rule.setLevel(1);
    rule.setType(TreeRuleType.METRIC);
    storage.addColumn(key, "tree_rule:1:0".getBytes(MockBase.ASCII()), 
        JSON.serializeToBytes(rule));
    
    root = new Branch(2);
    root.setDisplayName("ROOT");
    root_path = new TreeMap<Integer, String>();
    root_path.put(0, "ROOT");
    root.prependParentPath(root_path);
    storage.addColumn(key, "branch".getBytes(MockBase.ASCII()), 
        (byte[])branch_json.invoke(root));
    
    // sprinkle in some collisions and no matches for fun
    // collisions
    key = new byte[] { 0, 1, 1 };
    String tsuid = "010101";
    byte[] qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, qualifier, "AAAAAA".getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.COLLISION_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Tree.COLLISION_PREFIX(), 0, qualifier, 0, 
        Tree.COLLISION_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.COLLISION_PREFIX().length, 
        tsuid_bytes.length);
    storage.addColumn(key, qualifier, "BBBBBB".getBytes(MockBase.ASCII()));
    
    // not matched
    key = new byte[] { 0, 1, 2 };
    tsuid = "010101";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, qualifier, "Failed rule 0:0".getBytes(MockBase.ASCII()));
    
    tsuid = "020202";
    qualifier = new byte[Tree.NOT_MATCHED_PREFIX().length + 
                             (tsuid.length() / 2)];
    System.arraycopy(Tree.NOT_MATCHED_PREFIX(), 0, qualifier, 0, 
        Tree.NOT_MATCHED_PREFIX().length);
    tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Tree.NOT_MATCHED_PREFIX().length, 
    tsuid_bytes.length);
    storage.addColumn(key, qualifier, "Failed rule 1:1".getBytes(MockBase.ASCII()));
    
  }
  
  @Test
  public void handleTreeAll() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"2nd Tree\""));
  }
  
  @Test
  public void handleTreeSingle() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?tree=2");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertTrue(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"Test Tree\""));
    assertFalse(query.response().getContent().toString(MockBase.ASCII())
        .contains("\"name\":\"2nd Tree\""));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeNotFound() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?tree=3");
    this.rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreeQSCreate() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?name=NewTree&method=post");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(1, storage.numColumns(new byte[] { 0, 3 }));
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSCreateNoName() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?method=post");
    this.rpc.execute(tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void handleTreeQSCreateOutOfIDs() throws Exception {
    storage.addColumn(new byte[] { (byte) 0xFF, (byte) 0xFF }, 
        "tree".getBytes(MockBase.ASCII()), "{}".getBytes(MockBase.ASCII()));
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/tree?method=post");
    this.rpc.execute(tsdb, query);
  }
  
  @Test
  public void handleTreePOSTCreate() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, 
      "/api/tree", "{\"name\":\"New Tree\"}");
    this.rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
    assertEquals(1, storage.numColumns(new byte[] { 0, 3 }));
  }
}

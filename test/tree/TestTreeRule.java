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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.regex.PatternSyntaxException;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.MockBase;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, HBaseClient.class, GetRequest.class,
  PutRequest.class, KeyValue.class, Scanner.class, DeleteRequest.class, 
  Tree.class})
public final class TestTreeRule {
  private MockBase storage;
  private TreeRule rule;
  
  @Before
  public void before() {
    rule = new TreeRule();
  }
  
  @Test
  public void setRegex() {
    rule.setRegex("^HelloWorld$");
    assertNotNull(rule.getCompiledRegex());
    assertEquals("^HelloWorld$", rule.getCompiledRegex().pattern());
  }
  
  @Test (expected = PatternSyntaxException.class)
  public void setRegexBadPattern() {
    rule.setRegex("Invalid\\\\(pattern");
  }
  
  @Test
  public void setRegexNull() {
    rule.setRegex(null);
    assertNull(rule.getRegex());
    assertNull(rule.getCompiledRegex());
  }
  
  @Test
  public void setRegexEmpty() {
    rule.setRegex("");
    assertTrue(rule.getRegex().isEmpty());
    assertNull(rule.getCompiledRegex());
  }
  
  @Test
  public void stringToTypeMetric() {
    assertEquals(TreeRuleType.METRIC, TreeRule.stringToType("Metric"));
  }
  
  @Test
  public void stringToTypeMetricCustom() {
    assertEquals(TreeRuleType.METRIC_CUSTOM, 
        TreeRule.stringToType("Metric_Custom"));
  }
  
  @Test
  public void stringToTypeTagk() {
    assertEquals(TreeRuleType.TAGK, TreeRule.stringToType("TagK"));
  }
  
  @Test
  public void stringToTypeTagkCustom() {
    assertEquals(TreeRuleType.TAGK_CUSTOM, TreeRule.stringToType("TagK_Custom"));
  }
  
  @Test
  public void stringToTypeTagvCustom() {
    assertEquals(TreeRuleType.TAGV_CUSTOM, TreeRule.stringToType("TagV_Custom"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeNull() {
    TreeRule.stringToType(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeEmpty() {
    TreeRule.stringToType("");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void stringToTypeInvalid() {
    TreeRule.stringToType("NotAType");
  }
  
  @Test
  public void serialize() {
    rule.setField("host");
    final String json = JSON.serializeToString(rule);
    System.out.println(json);
    assertNotNull(json);
    assertTrue(json.contains("\"field\":\"host\""));
  }
  
  @Test
  public void deserialize() {
    final String json = "{\"type\":\"METRIC\",\"field\":\"host\",\"regex\":" +
    "\"^[a-z]$\",\"separator\":\".\",\"description\":\"My Description\"," +
    "\"notes\":\"Got Notes?\",\"display_format\":\"POP {ovalue}\",\"level\":1" +
    ",\"order\":2,\"customField\":\"\",\"regexGroupIdx\":1,\"treeId\":42," +
    "\"UnknownKey\":\"UnknownVal\"}";
    rule = JSON.parseToObject(json, TreeRule.class);
    assertNotNull(rule);
    assertEquals(42, rule.getTreeId());
    assertEquals("^[a-z]$", rule.getRegex());
    assertNotNull(rule.getCompiledRegex());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void deserializeBadRegexCompile() {
    final String json = "{\"type\":\"METRIC\",\"field\":\"host\",\"regex\":" +
    "\"^(ok$\",\"separator\":\".\",\"description\":\"My Description\"," +
    "\"notes\":\"Got Notes?\",\"display_format\":\"POP {ovalue}\",\"level\":1" +
    ",\"order\":2,\"customField\":\"\",\"regexGroupIdx\":1,\"treeId\":42," +
    "\"UnknownKey\":\"UnknownVal\"}";
    rule = JSON.parseToObject(json, TreeRule.class);
  }

  @Test
  public void fetchRule() throws Exception {
    setupStorage();
    final TreeRule rule = TreeRule.fetchRule(storage.getTSDB(), 1, 2, 1)
      .joinUninterruptibly();
    assertNotNull(rule);
    assertEquals(1, rule.getTreeId());
    assertEquals(2, rule.getLevel());
    assertEquals(1, rule.getOrder());
    assertEquals("Host owner", rule.getDescription());
  }
  
  @Test
  public void fetchRuleDoesNotExist() throws Exception {
    setupStorage();
    final TreeRule rule = TreeRule.fetchRule(storage.getTSDB(), 1, 2, 2)
      .joinUninterruptibly();
    assertNull(rule);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID0() throws Exception {
    setupStorage();
    TreeRule.fetchRule(storage.getTSDB(), 0, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID65536() throws Exception {
    setupStorage();
    TreeRule.fetchRule(storage.getTSDB(), 65536, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadLevel() throws Exception {
    setupStorage();
    TreeRule.fetchRule(storage.getTSDB(), 1, -1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadOrder() throws Exception {
    setupStorage();
    TreeRule.fetchRule(storage.getTSDB(), 1, 2, -1);
  }
  
  @Test
  public void storeRule() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC);
    rule.setNotes("Just some notes");
    assertTrue(rule.storeRule(storage.getTSDB(), false).joinUninterruptibly());
    assertEquals(3, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleTreeDoesNotExist() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(2);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false);
  }
  
  @Test
  public void storeRuleMege() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(2);
    rule.setOrder(1);
    rule.setNotes("Just some notes");
    assertTrue(rule.storeRule(storage.getTSDB(), false).joinUninterruptibly());
    assertEquals(2, storage.numColumns(new byte[] { 0, 1 }));
    final TreeRule stored = JSON.parseToObject(
        storage.getColumn(new byte[] { 0, 1 }, 
        "tree_rule:2:1".getBytes(MockBase.ASCII())), TreeRule.class);
    assertEquals("Host owner", stored.getDescription());
    assertEquals("Just some notes", stored.getNotes());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID0() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(0);
    rule.storeRule(storage.getTSDB(), false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID65536() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(65536);
    rule.storeRule(storage.getTSDB(), false);
  }
  
  @Test (expected = IllegalStateException.class)
  public void storeRuleNoChanges() throws Exception {
    setupStorage();
    final TreeRule rule = TreeRule.fetchRule(storage.getTSDB(), 1, 2, 1)
      .joinUninterruptibly();
    rule.storeRule(storage.getTSDB(), false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidType() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagk() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagkCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagvCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldMetricCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagkCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagvCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldMetricCustom() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidRegexIdx() throws Exception {
    setupStorage();
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*$");
    rule.setRegexGroupIdx(-1);
    rule.storeRule(storage.getTSDB(), false).joinUninterruptibly();
  }
  
  @Test
  public void deleteRule() throws Exception {
    setupStorage();
    assertNotNull(TreeRule.deleteRule(storage.getTSDB(), 1, 2, 1));
    assertEquals(1, storage.numColumns(new byte[] { 0, 1 }));
  }
  
  @Test
  public void deleteAllRules() throws Exception {
    setupStorage();
    TreeRule.deleteAllRules(storage.getTSDB(), 1);
    assertEquals(1, storage.numColumns(new byte[] { 0, 1 }));
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage() throws Exception {
    storage = new MockBase(true, true, true, true);

    final TreeRule stored_rule = new TreeRule(1);
    stored_rule.setLevel(2);
    stored_rule.setOrder(1);
    stored_rule.setType(TreeRuleType.METRIC_CUSTOM);
    stored_rule.setField("host");
    stored_rule.setCustomField("owner");
    stored_rule.setDescription("Host owner");
    stored_rule.setNotes("Owner of the host machine");
    
    // pretend there's a tree definition in the storage row
    storage.addColumn(new byte[] { 0, 1 }, "tree".getBytes(MockBase.ASCII()), 
        new byte[] { 1 });
    
    // add a rule to the row
    storage.addColumn(new byte[] { 0, 1 }, 
        "tree_rule:2:1".getBytes(MockBase.ASCII()),
        JSON.serializeToBytes(stored_rule));

    PowerMockito.spy(Tree.class);
    PowerMockito.doReturn(true).when(Tree.class, "treeExists", storage.getTSDB(), 1);
    PowerMockito.doReturn(false).when(Tree.class, "treeExists", storage.getTSDB(), 2);

  }
}

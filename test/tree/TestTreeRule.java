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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import net.opentsdb.core.TSDB;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.utils.JSON;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, HBaseClient.class, PutRequest.class, 
  KeyValue.class, DeleteRequest.class, Tree.class})
public final class TestTreeRule {
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private HashMap<String, HashMap<String, byte[]>> storage = 
    new HashMap<String, HashMap<String, byte[]>>();
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
    setupStorage(true);
    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 1);
    assertNotNull(rule);
    assertEquals(1, rule.getTreeId());
    assertEquals(2, rule.getLevel());
    assertEquals(1, rule.getOrder());
    assertEquals("Host owner", rule.getDescription());
  }
  
  @Test
  public void fetchRuleDoesNotExist() throws Exception {
    setupStorage(true);
    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 2);
    assertNull(rule);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID0() throws Exception {
    TreeRule.fetchRule(tsdb, 0, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadTreeID65536() throws Exception {
    TreeRule.fetchRule(tsdb, 65536, 2, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadLevel() throws Exception {
    TreeRule.fetchRule(tsdb, 1, -1, 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void fetchRuleBadOrder() throws Exception {
    TreeRule.fetchRule(tsdb, 1, 2, -1);
  }
  
  @Test
  public void storeRule() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC);
    rule.setNotes("Just some notes");
    assertTrue(rule.storeRule(tsdb, false).joinUninterruptibly());
    assertEquals(3, storage.get("0001").size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleTreeDoesNotExist() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(2);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false);
  }
  
  @Test
  public void storeRuleMege() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(2);
    rule.setOrder(1);
    rule.setNotes("Just some notes");
    assertTrue(rule.storeRule(tsdb, false).joinUninterruptibly());
    assertEquals(2, storage.get("0001").size());
    final TreeRule stored = JSON.parseToObject(
        storage.get("0001").get("tree_rule:2:1"), TreeRule.class);
    assertEquals("Host owner", stored.getDescription());
    assertEquals("Just some notes", stored.getNotes());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID0() throws Exception {
    final TreeRule rule = new TreeRule(0);
    rule.storeRule(tsdb, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleBadID65536() throws Exception {
    final TreeRule rule = new TreeRule(65536);
    rule.storeRule(tsdb, false);
  }
  
  @Test (expected = IllegalStateException.class)
  public void storeRuleNoChanges() throws Exception {
    setupStorage(true);
    final TreeRule rule = TreeRule.fetchRule(tsdb, 1, 2, 1);
    rule.storeRule(tsdb, false);
  }
  
  @Test
  public void storeRuleCASFalse() throws Exception {
    setupStorage(false);
    
    when (client.compareAndSet((PutRequest)any(), (byte[])any()))
    .thenAnswer(new Answer<Deferred<Boolean>>() {

      @Override
      public Deferred<Boolean> answer(final InvocationOnMock invocation) 
        throws Throwable {
        return Deferred.fromResult(false);
      }
      
    });
    
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC);
    rule.setNotes("Just some notes");
    assertFalse(rule.storeRule(tsdb, false).joinUninterruptibly());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidType() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagk() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagkCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldTagvCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingFieldMetricCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setNotes("Just some notes");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagkCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldTagvCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidMissingCustomFieldMetricCustom() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setNotes("Just some notes");
    rule.setField("foo");
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void storeRuleInvalidRegexIdx() throws Exception {
    setupStorage(true);
    final TreeRule rule = new TreeRule(1);
    rule.setLevel(1);
    rule.setOrder(0);
    rule.setType(TreeRuleType.TAGK);
    rule.setRegex("^.*$");
    rule.setRegexGroupIdx(-1);
    rule.storeRule(tsdb, false).joinUninterruptibly();
  }
  
  @Test
  public void deleteRule() throws Exception {
    setupStorage(true);
    assertNotNull(TreeRule.deleteRule(tsdb, 1, 2, 1));
    assertEquals(1, storage.get("0001").size());
  }
  
  @Test
  public void deleteAllRules() throws Exception {
    setupStorage(true);
    TreeRule.deleteAllRules(tsdb, 1);
    assertEquals(1, storage.get("0001").size());
  }

  /**
   * Mocks classes for testing the storage calls
   */
  private void setupStorage(final boolean default_put) throws Exception {
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());
    
    final Charset ascii = Charset.forName("ISO-8859-1");
    
    final TreeRule stored_rule = new TreeRule(1);
    stored_rule.setLevel(2);
    stored_rule.setOrder(1);
    stored_rule.setType(TreeRuleType.METRIC_CUSTOM);
    stored_rule.setField("host");
    stored_rule.setCustomField("owner");
    stored_rule.setDescription("Host owner");
    stored_rule.setNotes("Owner of the host machine");
    
    // pretend there's a tree definition in the storage row
    HashMap<String, byte[]> col = new HashMap<String, byte[]>();
    col.put("tree", new byte[] { 1 });
    
    // add a rule to the row
    col.put("tree_rule:2:1", JSON.serializeToBytes(stored_rule));
    storage.put("0001", col);

    PowerMockito.spy(Tree.class);
    PowerMockito.doReturn(true).when(Tree.class, "treeExists", tsdb, 1);
    PowerMockito.doReturn(false).when(Tree.class, "treeExists", tsdb, 2);
    
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
    
    when(client.delete((DeleteRequest)any())).then(
        new Answer<Deferred<Object>>() {

        @Override
        public Deferred<Object> answer(InvocationOnMock invocation)
            throws Throwable {
          final Object[] args = invocation.getArguments();
          final DeleteRequest delete = (DeleteRequest)args[0];
          final String key = Branch.idToString(delete.key());
          final String qualifier = new String(delete.qualifiers()[0]);
          HashMap<String, byte[]> column = storage.get(key);
          if (column == null) {
            return Deferred.fromResult(new Object());
          }
          column.remove(qualifier);
          return Deferred.fromResult(new Object());
        }
      
    });
  }
}

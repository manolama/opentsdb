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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;

import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
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
@PrepareForTest({TSDB.class, Branch.class, RowLock.class, PutRequest.class, 
  HBaseClient.class, Tree.class})
public final class TestTreeBuilder {
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private Tree tree = TestTree.buildTestTree();
  private TreeBuilder treebuilder = new TreeBuilder(tsdb);
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
  private HashMap<String, HashMap<String, byte[]>> storage = 
    new HashMap<String, HashMap<String, byte[]>>();
  private String leaf_qualifier;
  
  @Before
  public void before() throws Exception {
    PowerMockito.spy(Tree.class);
    PowerMockito.doReturn(tree).when(Tree.class, "fetchTree", 
        (TSDB)any(), anyInt());
    
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

    final byte[] qualifier = new byte[Leaf.LEAF_PREFIX().length + 
                                      (tsuid.length() / 2)];
    System.arraycopy(Leaf.LEAF_PREFIX(), 0, qualifier, 0, 
        Leaf.LEAF_PREFIX().length);
    final byte[] tsuid_bytes = UniqueId.stringToUid(tsuid);
    System.arraycopy(tsuid_bytes, 0, qualifier, Leaf.LEAF_PREFIX().length, 
        tsuid_bytes.length);
    leaf_qualifier = new String(qualifier);
    
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes());

    // return the root when asked
    PowerMockito.spy(Branch.class);
    PowerMockito.doAnswer(new Answer<Branch>() {
      @Override
      public Branch answer(InvocationOnMock invocation) throws Throwable {
        return TestBranch.buildTestBranch(tree);
      }
    }).when(Branch.class, "fetchBranch", (TSDB)any(), (byte[])any(), 
        anyBoolean());

    doAnswer(new Answer<Deferred<Object>>() {
        @Override
        public Deferred<Object> answer(InvocationOnMock invocation) 
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
          System.out.println("Stored: " + Branch.idToString(put.key()) + "  Q: " + 
              new String(put.qualifier()));
          return Deferred.fromResult(new Object());
        }
      }).when(client)
      .compareAndSet((PutRequest) any(), (byte[])any());
  }
  
  @Test
  public void processTimeseriesMetaDefaults() throws Exception {
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
        "00010001A2460001CB54247F72020001BECD000181A800000030").size());
    final Branch branch = JSON.parseToObject(storage.get(
        "00010001A2460001CB54247F72020001BECD000181A800000030").get("branch"), 
        Branch.class);
    assertNotNull(branch);
    assertEquals("0", branch.getDisplayName());
    final Leaf leaf = JSON.parseToObject(storage.get(
    "00010001A2460001CB54247F72020001BECD000181A800000030")
    .get(leaf_qualifier), Leaf.class);
    assertNotNull(leaf);
    assertEquals("user", leaf.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaNewRoot() throws Exception {
    PowerMockito.doAnswer(new Answer<Branch>() {
      @Override
      public Branch answer(InvocationOnMock invocation) throws Throwable {
        return null;
      }
    }).when(Branch.class, "fetchBranch", (TSDB)any(), (byte[])any(), 
        anyBoolean());
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(7, storage.size());
    assertEquals(1, storage.get("0001").size());
  }

  @Test
  public void processTimeseriesMetaMiddleNonMatchedRules() throws Exception {
    // tests to make sure we collapse branches if rules at the front or middle
    // of the rule set are not matched
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(0);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(1);
    rule.setOrder(1);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(4, storage.size());
    assertEquals(2, storage.get("0001247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaEndNonMatchedRules() throws Exception {
    // tests to make sure we collapse branches if rules at the end
    // of the rule set are not matched
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(5);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(5);
    rule.setOrder(1);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(6);
    tree.addRule(rule);
    
    rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("dept");
    rule.setDescription("Department");
    rule.setLevel(6);
    rule.setOrder(1);
    tree.addRule(rule);
    treebuilder.setTree(tree);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void processTimeseriesMetaNullMeta() throws Exception {
    treebuilder.processTimeseriesMeta(1, null, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void processTimeseriesMetaTreeId0() throws Exception {
    treebuilder.processTimeseriesMeta(0, meta, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void processTimeseriesMetaTreeId255() throws Exception {
    treebuilder.processTimeseriesMeta(255, meta, false);
  }

  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaMetric() throws Exception {
    Field tag_metric = TSMeta.class.getDeclaredField("metric");
    tag_metric.setAccessible(true);
    tag_metric.set(meta, null);
    tag_metric.setAccessible(false);
    treebuilder.processTimeseriesMeta(1, meta, false);
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaNullMetaTags() throws Exception {
    Field tags = TSMeta.class.getDeclaredField("tags");
    tags.setAccessible(true);
    tags.set(meta, null);
    tags.setAccessible(false);
    treebuilder.processTimeseriesMeta(1, meta, false);
  }
  
  @Test
  public void processTimeseriesMetaNullMetaOddNumTags() throws Exception {
    ArrayList<UIDMeta> tags = new ArrayList<UIDMeta>(4);
    tags.add(tagk1);
    //tags.add(tagv1); <-- whoops. This will process through but missing host
    tags.add(tagk2);
    tags.add(tagv2);
    Field tags_field = TSMeta.class.getDeclaredField("tags");
    tags_field.setAccessible(true);
    tags_field.set(meta, tags);
    tags_field.setAccessible(false);
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(4, storage.size());
    assertEquals(2, storage.get(
      "00010036EBCB0001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaTesting() throws Exception {
    treebuilder.processTimeseriesMeta(1, meta, true);
    assertEquals(0, storage.size());
  }

  @Test
  public void processTimeseriesMetaStrict() throws Exception {
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaStrictNoMatch() throws Exception {
    Field name = UIDMeta.class.getDeclaredField("name");
    name.setAccessible(true);
    name.set(tagv1, "foobar");
    name.setAccessible(false);
    tree.setStrictMatch(true);
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(0, storage.size());
  }

  @Test
  public void processTimeseriesMetaNoSplit() throws Exception {
    tree.getRules().get(3).get(0).setSeparator("");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(4, storage.size());
    assertEquals(2, storage.get("00010001A2460001CB54247F7202CBBF5B09").size());
  }
  
  @Test
  public void processTimeseriesMetaInvalidRegexIdx() throws Exception {
    tree.getRules().get(1).get(1).setRegexGroupIdx(42);
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(5, storage.size());
    assertEquals(2, storage.get(
        "00010001A246247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaMetricCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "0001AE805CA50001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaMetricCustomNullValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);    
  }
  
  @Test
  public void processTimeseriesMetaMetricCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    metric.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.METRIC_CUSTOM);
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaTagkCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "0001AE805CA50001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaTagkCustomNull() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);    
  }
  
  @Test
  public void processTimeseriesMetaTagkCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaTagkCustomNoField() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagk1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGK_CUSTOM);
    //rule.setField("host"); <-- must be set to match
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaTagvCustom() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("web-01.lga.mysite.com");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "0001AE805CA50001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void processTimeseriesMetaTagvCustomNullValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", null);
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("web-01.lga.mysite.com");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);    
  }
  
  @Test
  public void processTimeseriesMetaTagvCustomEmptyValue() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    rule.setField("host");
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaTagvCustomNoField() throws Exception {
    HashMap<String, String> custom = new HashMap<String, String>();
    custom.put("owner", "John Doe");
    custom.put("dc", "lga");
    tagv1.setCustom(custom);
    
    TreeRule rule = new TreeRule(1);
    rule.setType(TreeRuleType.TAGV_CUSTOM);
    //rule.setField("host"); <-- must be set to match
    rule.setCustomField("owner");
    rule.setDescription("Owner");
    rule.setLevel(0);
    tree.addRule(rule);
    
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    assertEquals(2, storage.get(
      "00010001A2460001CB54247F72020001BECD000181A800000030").size());
  }
  
  @Test
  public void processTimeseriesMetaFormatOvalue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("OV: {ovalue}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A24637E140D5").get("branch"), Branch.class);
    assertEquals("OV: web-01.lga.mysite.com", branch.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaFormatValue() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("V: {value}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A24696026FD8").get("branch"), Branch.class);
    assertEquals("V: web", branch.getDisplayName());
  }
  
  @Test
  public void processTimeseriesMetaFormatTSUID() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TSUID: {tsuid}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A246E0A07086").get("branch"), Branch.class);
    assertEquals("TSUID: " + tsuid, branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatTagName() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat("TAGNAME: {tag_name}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A2467BFCCB13").get("branch"), Branch.class);
    assertEquals("TAGNAME: host", branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatMulti() throws Exception {
    tree.getRules().get(1).get(1).setDisplayFormat(
        "{ovalue}:{value}:{tag_name}:{tsuid}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(6, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A246E4592083").get("branch"), Branch.class);
    assertEquals("web-01.lga.mysite.com:web:host:0102030405", 
        branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatBadType() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("Wrong: {tag_name}");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(4, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A2460001CB54247F7202C3165573").get("branch"), 
        Branch.class);
    assertEquals("Wrong: ", branch.getDisplayName());    
  }
  
  @Test
  public void processTimeseriesMetaFormatOverride() throws Exception {
    tree.getRules().get(3).get(0).setDisplayFormat("OVERRIDE");
    treebuilder.processTimeseriesMeta(1, meta, false);
    assertEquals(4, storage.size());
    final Branch branch = JSON.parseToObject(
        storage.get("00010001A2460001CB54247F72024E3D0BCC").get("branch"), Branch.class);
    assertEquals("OVERRIDE", branch.getDisplayName());    
  }
}

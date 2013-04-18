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

import java.util.regex.PatternSyntaxException;

import net.opentsdb.tree.TreeRule;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.utils.JSON;

import org.junit.Before;
import org.junit.Test;

public final class TestTreeRule {
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
}

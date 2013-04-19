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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.JSONException;

import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods to compile a tree given a single TSUID or more.
 * @since 2.0
 */
public final class TreeBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TreeBuilder.class);
  
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** UID table row where tree definitions are stored */
  private static byte[] TREE_DEFINITION_ROW = { 0x01, 0x00 };
  /** The prefix to use when addressing branch storage rows */
  private static byte TREE_BRANCH_PREFIX = 0x01;
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  
  /** The TSDB to use for fetching/writing data */
  private final TSDB tsdb;
  
  /** Stores parsed branches ready for synchronization with storage */
  private HashMap<Integer, Branch> temp_branches = 
    new HashMap<Integer, Branch>();
  
  /** 
   * Used when parsing data to determine the max rule ID, necessary when users
   * skip a level on accident
   */
  private int max_rule_level;
  
  /** Whether or not the ruleset requires we load meta data before parsing */
  private boolean load_meta;
  
  /** Filled with messages when the user has asked for a test run */
  private ArrayList<String> test_messages;
  
  /** The tree to work with */
  private Tree tree;
  
  /** The meta data we're parsing */
  private TSMeta meta;
  
  /** Current array of splits, may be null */
  private String[] splits;
  
  /** Current rule index */
  private int rule_idx;
  
  /** Current split index */
  private int split_idx;

  /** The current branch we're working with */
  private Branch current_branch;
  
  /** Current rule */
  private TreeRule rule;
  
  /** Whether or not the TS failed to match a rule, used for 
   * {@code strict_match} */
  private boolean had_nomatch;
  
  /**
   * Default constructor requires a TSDB
   * @param tsdb The TSDB to use for storage interaction
   */
  public TreeBuilder(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  

  private void processTimeseries(final Branch parent_branch, final int depth) {
    // when we've passed the final rule, just return to stop the recursion
    if (rule_idx > max_rule_level) {
      return;
    }
    
    // setup the branch for this iteration and set the "current_branch" 
    // reference. It's not final as we'll be copying references back and forth
    Branch local_branch = new Branch();
    if (parent_branch != null) {
      local_branch.setParentBranchId(parent_branch.getBranchId());
    }
    local_branch.setDepth(depth);
    current_branch = local_branch;
    
    // fetch the current rule level or try to find the next one
    TreeMap<Integer, TreeRule> rule_level = getCurrentRuleLevel();
    if (rule_level == null) {
      return;
    }
    
    // loop through each rule in the level, processing as we go
    for (Map.Entry<Integer, TreeRule> entry : rule_level.entrySet()) {
      // set the local rule
      rule = entry.getValue();
      testMessage("Processing rule: " + rule.toStringId());
      
      // route to the proper handler based on the rule type
      if (rule.getType() == TreeRuleType.METRIC) {
        processMetricRule(parent_branch);
        // local_branch = current_branch; //do we need this???
      } else if (rule.getType() == TreeRuleType.TAGK) {
        processTagkRule(parent_branch);
      } else if (rule.getType() == TreeRuleType.METRIC_CUSTOM) {
        processMetricCustomRule(parent_branch);
      } else if (rule.getType() == TreeRuleType.TAGK_CUSTOM) {
        
      }
    }
  }
  
  private void processMetricRule(final Branch parent_branch) {
    if (meta.getMetric() == null) {
      throw new IllegalStateException(
          "Timeseries metric UID object was null");
    }
    final String metric = meta.getMetric().getName();
    if (metric == null || metric.isEmpty()) {
      throw new IllegalStateException(
          "Timeseries metric name was null or empty");
    }
    processParsedValue(parent_branch, metric);
  }

  private void processTagkRule(final Branch parent_branch) {
    final ArrayList<UIDMeta> tags = meta.getTags();
    if (tags == null || tags.isEmpty()) {
      throw new IllegalStateException(
        "Tags for the timeseries meta were null");
    }
    
    String tag_name = "";
    boolean found = false;
    
    // loop through each tag pair. If the tagk matches the requested field name
    // then we flag it as "found" and on the next pass, grab the tagv name. This
    // assumes we have a list of [tagk, tagv, tagk, tagv...] pairs. If not, 
    // we're screwed
    for (UIDMeta uidmeta : tags) {
      if (uidmeta.getType() == UniqueIdType.TAGK && 
          uidmeta.getName().equals(rule.getField())) {
        found = true;
      } else if (uidmeta.getType() == UniqueIdType.TAGV && found) {
        tag_name = uidmeta.getName();
        break;
      }
    }
    
    // if we didn't find a match, return
    if (!found || tag_name.isEmpty()) {
      testMessage("No match on tagk [" + rule.getField() + "] for rule: " + 
          rule.toStringId());
      return;
    }
    
    // matched!
    processParsedValue(parent_branch, tag_name);
    testMessage("Matched tagk [" + rule.getField() + "] for rule: " + 
        rule.toStringId());
  }
  
  private void processMetricCustomRule(final Branch parent_branch) {
    if (meta.getMetric() == null) {
      throw new IllegalStateException(
          "Timeseries metric UID object was null");
    }
    Map<String, String> custom = meta.getMetric().getCustom();
    if (custom != null && !custom.isEmpty() && 
        !custom.containsKey(rule.getCustomField())) {
      processParsedValue(parent_branch, custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule.toStringId());
    } else {
      // no match
      testMessage("No match on custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule.toStringId());
    }
  }
  
  private void processParsedValue(final Branch parent_branch, 
      final String parsed_value) {
    if (rule.getCompiledRegex() == null && 
        (rule.getSeparator() == null || rule.getSeparator().isEmpty())) {
      // we don't have a regex and we don't need to separate, so just use the
      // name of the timseries
      setCurrentName(parent_branch, parsed_value, parsed_value);
    } else if (rule.getCompiledRegex() != null) {
      // we have a regex rule, so deal with it
      processRegexRule(parent_branch, parsed_value);
    } else if (rule.getSeparator() != null && !rule.getSeparator().isEmpty()) {
      // we have a split rule, so deal with it
      processSplit(parent_branch, parsed_value);
    } else {
      throw new IllegalStateException("Unable to find a processor for rule: " + 
          rule.toStringId());
    }
  }
  
  private void processSplit(final Branch parent_branch, final String value) {
    if (splits == null) {
      // then this is the first time we're processing the value, so we need to
      // execute the split if there's a separator, after some validation
      if (value == null || value.isEmpty()) {
        throw new IllegalArgumentException("Value was empty for rule: " + 
            rule.toStringId());
      }
      if (rule.getSeparator() == null || rule.getSeparator().isEmpty()) {
        throw new IllegalArgumentException("Separator was empty for rule: " + 
            rule.toStringId());
      }      
      
      // split it
      splits = value.split(rule.getSeparator());
      split_idx = 0;
      setCurrentName(parent_branch, value, splits[split_idx]);
      split_idx++;
    } else {
      // otherwise we have split values and we just need to grab the next one
      setCurrentName(parent_branch, value, splits[split_idx]);
      split_idx++;
    }
  }
  
  private void processRegexRule(final Branch parent_branch, 
      final String matched_value) {
    if (rule.getCompiledRegex() == null) {
      throw new IllegalArgumentException("Regex was null for rule: " + 
          rule.toStringId());
    }
    
    final Matcher matcher = rule.getCompiledRegex().matcher(matched_value);
    if (matcher.find()) {
      // The first group is always the full string, so we need to increment
      // by one to fetch the proper group
      if (matcher.groupCount() >= rule.getRegexGroupIdx() + 1) {
        final String extracted = 
          matcher.group(rule.getRegexGroupIdx() + 1);
        if (extracted == null || extracted.isEmpty()) {
          // can't use empty values as a branch/leaf name
          testMessage("Extracted value for rule " + 
              rule.toStringId() + " was null or empty");
        } else {
          // found a branch or leaf!
          setCurrentName(parent_branch, matched_value, extracted);
        }
      } else {
        // the group index was out of range
        testMessage("Regex group index [" + 
            rule.getRegexGroupIdx() + "] for rule " + 
            rule.toStringId() + " was out of bounds [" +
            matcher.groupCount() + "]");
      }
    }
  }
   
  /**
   * 
   * @param original_value The original, raw value processed by the calling rule
   * @param parsed_value The post-processed value after the rule worked on it
   */
  private void setCurrentName(final Branch parent_branch, 
      final String original_value, final String parsed_value) {

    // set the private name first, the one with the separator
    if (parent_branch == null || parent_branch.getName().isEmpty()) {
      current_branch.setName(parsed_value);
    } else {
      current_branch.setName(parent_branch.getName() + 
          tree.getNodeSeparator() + parsed_value);
    }
    
    // now parse and set the display name. If the formatter is empty, we just 
    // set it to the parsed value and exit
    String format = rule.getDisplayFormat();
    if (format == null || format.isEmpty()) {
      current_branch.setDisplayName(parsed_value);
      return;
    }
    
    if (format.contains("{ovalue}")) {
      format = format.replace("{ovalue}", original_value);
    }
    if (format.contains("{value}")) {
      format = format.replace("{value}", parsed_value);
    }
    if (format.contains("{tsuid}")) {
      format = format.replace("{tsuid}", meta.getTSUID());
    }
    if (format.contains("{tag_name}")) {
      final TreeRuleType type = rule.getType();
      if (type == TreeRuleType.TAGK) {
        format = format.replace("{tag_name}", rule.getField());
      } else if (type == TreeRuleType.METRIC_CUSTOM ||
          type == TreeRuleType.TAGK_CUSTOM ||
          type == TreeRuleType.TAGV_CUSTOM) {
        format = format.replace("{tag_name}", rule.getCustomField());
      } else {
        // we can't match the {tag_name} token since the rule type is invalid
        // so we'll just blank it
        format = format.replace("{tag_name}", "");
        LOG.warn("Display rule " + rule.toStringId() + 
          " was of the wrong type to match on {tag_name}");
        if (test_messages != null) {
          test_messages.add("Display rule " + rule.toStringId() + 
              " was of the wrong type to match on {tag_name}");
        }
      }
    }
    current_branch.setDisplayName(format);
  }
  
  /**
   * Helper method that iterates through the first dimension of the rules map
   * to determine the highest level (or key) and stores it to 
   * {@code max_rule_level}
   */
  private void calculateMaxLevel() {
    if (tree.getRules() == null) {
      LOG.debug("No rules set for this tree");
      return;
    }
    
    for (Integer level : tree.getRules().keySet()) {
      if (level > max_rule_level) {
        max_rule_level = level;
      }
    }
  }
  
  private void testMessage(final String message) {
    if (test_messages != null) {
      test_messages.add(message);
    }
    LOG.trace(message);
  }
  
  private TreeMap<Integer, TreeRule> getCurrentRuleLevel() {
    TreeMap<Integer, TreeRule> current_level = null;
    
    // iterate until we find some rules on a level or we run out
    while (current_level == null && rule_idx <= max_rule_level) {
      current_level = tree.getRules().get(rule_idx);
      if (current_level != null) {
        return current_level;
      } else {
        rule_idx++;
      }
    }
    
    // no more levels
    return null;
  }
  
  /**
   * Attempts to retrieve the branch from storage, optionally with a lock
   * @param branch_id The ID of the branch to fetch
   * @param lock An optional lock if performing an atomic sync
   * @return The branch from storage
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the data was corrupted
   */
  private Branch fetchBranch(final int branch_id, final RowLock lock) {
    // row ID = [ prefix, tree_id ] so we just get the tree ID as a single byte
    final byte[] key = { TREE_BRANCH_PREFIX, 
        ((Integer)tree.getTreeId()).byteValue() };
    final GetRequest get = new GetRequest(tsdb.uidTable(), key);
    get.family(NAME_FAMILY);
    get.qualifier(Bytes.fromInt(branch_id));
    if (lock != null) {
      get.withRowLock(lock);
    }
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      return JSON.parseToObject(row.get(0).value(), Branch.class);
    } catch (HBaseException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (JSONException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Attempts to write the branch to storage with a lock on the row
   * @param branch The branch to write to storage
   * @throws HBaseException if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws JSONException if the object could not be serialized
   */
  private void storeBranch(final Branch branch) {       
    // row ID = [ prefix, tree_id ] so we just get the tree ID as a single byte
    final byte[] row = { TREE_BRANCH_PREFIX, 
        ((Integer)tree.getTreeId()).byteValue() };  
    
    final RowLock lock = tsdb.hbaseAcquireLock(tsdb.uidTable(), row, (short)3);
    try {
      final PutRequest put = new PutRequest(tsdb.uidTable(), row, NAME_FAMILY, 
          Bytes.fromInt(branch.getBranchId()), branch.toJson(true), lock);
      tsdb.hbasePutWithRetry(put, (short)3, (short)800);
    } finally {
      // release the lock!
      try {
        tsdb.getClient().unlockRow(lock);
      } catch (HBaseException e) {
        LOG.error("Error while releasing the lock on tree row: " + 
            tree.getTreeId(), e);
      }
    }
  }
  
  /**
   * Compiles the branch name given a branch and value. 
   * It builds on the name via "name + separator + value"
   * @param branch The branch to parse the name from
   * @param value The processed value at a rule level
   * @return The value to use for the branch name
   */
  private String setBranchName(final Branch branch, final String value) {
    if (branch == null || branch.getName().isEmpty()) {
      return value;
    }
    return branch.getName() + tree.getNodeSeparator() + value;
  }
}

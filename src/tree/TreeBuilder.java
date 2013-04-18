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
  
  /**
   * Default constructor requires a TSDB
   * @param tsdb The TSDB to use for storage interaction
   */
  public TreeBuilder(final TSDB tsdb) {
    this.tsdb = tsdb;
  }
  
  private RecursionState processRegexRule(RecursionState state, 
      final Branch branch, final TreeRule rule, final String value, 
      final TSMeta meta) {
    if (rule.getCompiledRegex() == null) {
      throw new IllegalArgumentException("Regex was null for rule: " + 
          rule.toStringId());
    }
    
    final Matcher matcher = rule.getCompiledRegex().matcher(value);
    if (matcher.find()) {
      // The first group is always the full string, so we need to increment
      // by one to fetch the proper group
      if (matcher.groupCount() >= rule.getRegexGroupIdx() + 1) {
        final String extracted = matcher.group(rule.getRegexGroupIdx() + 1);
        if (extracted == null || extracted.isEmpty()) {
          // can't use empty values as a branch/leaf name
          if (test_messages != null) {
            test_messages.add("Extracted value for rule " + rule.toStringId() +
                " was null or empty");
          }
        } else {
          // found a branch or leaf!
          state.current_branch.setName(setBranchName(branch, extracted));
          state.current_branch.setDisplayName(
              this.processDisplayFormatter(rule, meta, value, extracted));
        }
      } else {
        // the group index was out of range
        if (test_messages != null) {
          test_messages.add("Regex group index [" + rule.getRegexGroupIdx() + 
              "] for rule " + rule.toStringId() + " was out of bounds [" +
              matcher.groupCount() + "]");
        }
      }
    }
    return state;
  }
  
  /**
   * Returns a formatted {@code display_name} value to use in the branch/leaf
   * as per the format definition in the rule. If the {@code display_name} value
   * is null or empty, then the {@code parsed_value} will be returned. Otherwise
   * the tokens in the display formatter are replaced with the proper values.
   * @param rule Rule with the display formatter
   * @param meta TSMeta object we're working on
   * @param original_value The original, raw value processed by the calling rule
   * @param parsed_value The post-processed value after the rule worked on it
   * @return A string to be used for display purposes
   */
  private String processDisplayFormatter(final TreeRule rule, final TSMeta meta, 
      final String original_value, final String parsed_value) {
    String format = rule.getDisplayFormat();
    if (format == null || format.isEmpty()) {
      return parsed_value;
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
    
    return format;
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
  
  /**
   * Keeps track of the recursion state for a branch as we process a rule set
   * @since 2.0
   */
  final private class RecursionState {
    /** Current array of splits, may be null */
    String[] splits;
    
    /** Current rule index */
    int rule_idx;
    
    /** Current split index */
    int split_idx;
    
    /** The current branch we're working with */
    Branch current_branch;
  }
}

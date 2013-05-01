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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.tree.TreeRule.TreeRuleType;
import net.opentsdb.uid.UniqueId.UniqueIdType;

import org.hbase.async.HBaseException;
import org.hbase.async.RowLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods to compile a tree given a single TSUID or more.
 * @since 2.0
 */
public final class TreeBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TreeBuilder.class);

  private static ArrayList<TreeBuilder> builder_cache;
  
  private static long last_cache_load;
  
  private static int cache_timeout = 30;
  
  /** The TSDB to use for fetching/writing data */
  private final TSDB tsdb;
  
  /** Stores merged branches for testing */
  private Branch root;
  
  /** 
   * Used when parsing data to determine the max rule ID, necessary when users
   * skip a level on accident
   */
  private int max_rule_level;

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
  
  public void processTimeseriesMeta(final int tree_id, final TSMeta meta, 
      final boolean is_testing) {
    if (tree_id < 1 || tree_id > 254) {
      throw new IllegalArgumentException("Invalid tree ID");
    }
    if (meta == null || meta.getTSUID() == null || meta.getTSUID().isEmpty()) {
      throw new IllegalArgumentException("Missing TSUID");
    }
    
    // load the tree if we need to. The caller may re-use this object for many
    // timeseries.
    if (tree == null || tree.getTreeId() != tree_id) {
      tree = Tree.fetchTree(tsdb, tree_id);
      if (tree == null) {
        throw new IllegalArgumentException("Unable to locate tree for ID: " + 
            tree_id);
      }
      
      // set the max rule level before proceeding
      calculateMaxLevel();
    }
    
    // reset the state in case the caller is reusing 
    resetState();
    this.meta = meta;
    
    // Fetch or initialize the root branch
    // The root shouldn't change so we should be able to load this once
    // per tree.
    root = Branch.fetchBranch(tsdb, Tree.idToBytes(tree.getTreeId()), 
        false);
    if (root == null) {
      LOG.info("Couldn't find the root branch, initializing");
      root = new Branch(tree.getTreeId());
      root.setDisplayName("ROOT");
      final TreeMap<Integer, String> root_path = new TreeMap<Integer, String>();
      root_path.put(0, "ROOT");
      root.prependParentPath(root_path);
      if (!is_testing) {
        root.storeBranch(tsdb, tree, true);
        LOG.info("Initialized root branch for tree: " + tree.getTreeId());
      }
    }
    
    System.out.println("Root: " + root + " Path: " + root.getPath());
    // process with the depth set to 1 since we're a branch of the root
    processRuleset(root, 1);
    
    // if the tree has strict matching enabled and there was one or more level
    // that failed to match, we need to ignore the branch
    if (had_nomatch && tree.getStrictMatch()) {
      testMessage(
          "TSUID failed to match one or more rule levels, will not add: " + 
          meta);
    } else if (current_branch == null) {
      LOG.warn("Processed TSUID [" + meta + "] resulted in a null branch");
    } else if (!is_testing) {
      // store the tree and leaves
      Branch cb = current_branch;
      Map<Integer, String> path = root.getPath();
      cb.prependParentPath(path);
      while (cb != null) {
        cb.storeBranch(tsdb, tree, true);
        if (cb.getBranches() == null) {
          cb = null;
        } else {
          path = cb.getPath();
          // we should only have one child if we're building
          cb = cb.getBranches().first();
          cb.prependParentPath(path);
        }
      }
      
      // if we have collisions or no matches, flush em
      if (tree.getCollisions() != null && !tree.getCollisions().isEmpty()) {
        tree.storeTree(tsdb, false);
      }
      if (tree.getNotMatched() != null && !tree.getNotMatched().isEmpty()) {
        tree.storeTree(tsdb, false);
      }
    } else {
      // we are testing, so compile for display
      Branch cb = current_branch;
      root.addChild(cb);
      Map<Integer, String> path = root.getPath();
      cb.prependParentPath(path);
      while (cb != null) {
        if (cb.getBranches() == null) {
          cb = null;
        } else {
          path = cb.getPath();
          // we should only have one child if we're building
          cb = cb.getBranches().first();
          cb.prependParentPath(path);
        }
      }
    }
  }
  
  public static void processTSMeta(final TSDB tsdb, final TSMeta meta) {
    if ((System.currentTimeMillis() / 1000) - last_cache_load > cache_timeout) {
      
      // load trees
      builder_cache = null;
      final List<Tree> trees = Tree.fetchAllTrees(tsdb);
      if (trees != null && !trees.isEmpty()) {
        builder_cache = new ArrayList<TreeBuilder>(trees.size());
        for (Tree tree : trees) {
          final TreeBuilder builder = new TreeBuilder(tsdb);
          builder.setTree(tree);
          builder_cache.add(builder);
        }
      }
      
      last_cache_load = System.currentTimeMillis() / 1000;
    }
    
    // no trees have been configured
    if (builder_cache == null || builder_cache.isEmpty()) {
      return;
    }
    
    for (TreeBuilder builder : builder_cache) {
      try {
        builder.processTimeseriesMeta(builder.getTree().getTreeId(), 
            meta, false);
      } catch (Exception e) {
        LOG.error("Runtime Exception", e);
      }
    }
  }
  
  private boolean processRuleset(final Branch parent_branch, int depth) {
    System.out.println("[" + depth + "] PB: " + parent_branch);
    // when we've passed the final rule, just return to stop the recursion
    if (rule_idx > max_rule_level) {
      LOG.trace("Finished the rules: " + rule_idx + " of: " + max_rule_level);
      return true;
    }
    
    // setup the branch for this iteration and set the "current_branch" 
    // reference. It's not final as we'll be copying references back and forth
    final Branch previous_branch = current_branch;
    current_branch = new Branch(tree.getTreeId());
    
    // fetch the current rule level or try to find the next one
    TreeMap<Integer, TreeRule> rule_level = getCurrentRuleLevel();
    if (rule_level == null) {
      LOG.trace("Couldn't find another rule level");
      return true;
    }
    
    // loop through each rule in the level, processing as we go
    for (Map.Entry<Integer, TreeRule> entry : rule_level.entrySet()) {
      // set the local rule
      rule = entry.getValue();
      testMessage("Processing rule: " + rule);
      
      // route to the proper handler based on the rule type
      if (rule.getType() == TreeRuleType.METRIC) {
        parseMetricRule(parent_branch);
        // local_branch = current_branch; //do we need this???
      } else if (rule.getType() == TreeRuleType.TAGK) {
        parseTagkRule(parent_branch);
      } else if (rule.getType() == TreeRuleType.METRIC_CUSTOM) {
        parseMetricCustomRule(parent_branch);
      } else if (rule.getType() == TreeRuleType.TAGK_CUSTOM) {
        parseTagkCustomRule(parent_branch);
      } else if (rule.getType() == TreeRuleType.TAGV_CUSTOM) {
        parseTagvRule(parent_branch);
      } else {
        throw new IllegalArgumentException("Unkown rule type: " + 
            rule.getType());
      }
      
      // rules on a given level are ORd so the first one that matches, we bail
      if (current_branch.getDisplayName() != null && 
          !current_branch.getDisplayName().isEmpty()) {
        break;
      }
    }
    
    // if no match was found on the level, then we need to set no match
    if (current_branch.getDisplayName() == null || 
        current_branch.getDisplayName().isEmpty()) {
      had_nomatch = true;
    }
    
    // determine if we need to continue processing splits, are done with splits
    // or need to increment to the next rule level
    if (splits != null && split_idx >= splits.length) {
      // finished split processing
      splits = null;
      split_idx = 0;
      rule_idx++;
    } else if (splits != null) {
      // we're still processing splits, so continue
    } else {
      // didn't have any splits so continue on to the next level
      rule_idx++;
    }
    
    // call ourselves recursively until we hit a leaf or run out of rules
    final boolean complete = processRuleset(current_branch, ++depth);
    
    // if the recursion loop is complete, we either have a leaf or need to roll
    // back
    if (complete) {
      // if the current branch is null or empty, we didn't match, so roll back
      // to the previous branch and tell it to be the leaf
      if (current_branch == null || current_branch.getDisplayName() == null || 
          current_branch.getDisplayName().isEmpty()) {
        LOG.trace("Got to a null branch");
        current_branch = previous_branch;
        return true;
      }
      
      // if the parent has an empty ID, we need to roll back till we find one
      if (parent_branch.getDisplayName() == null || parent_branch.getDisplayName().isEmpty()) {
        testMessage("Depth [" + depth + "] Parent branch was empty, rolling back");
        return true;
      }
      
      // add the leaf to the parent and roll back
      final Leaf leaf = new Leaf(current_branch.getDisplayName(), meta.getTSUID());
      parent_branch.addLeaf(leaf, tree);
      testMessage("Depth [" + depth + "] Adding leaf [" + leaf + 
          "] to parent branch [" + parent_branch + "]");
      current_branch = previous_branch;
      return false;
    }
    
    // if a rule level failed to match, we just skip the result swap
    if ((previous_branch == null || previous_branch.getDisplayName().isEmpty()) && 
        !current_branch.getDisplayName().isEmpty()) {
      if (depth > 2) {
        testMessage("Depth [" + depth + "] Skipping a non-matched branch, returning: " + current_branch);
      }
      return false;
    }

    // if the current branch is empty, skip it
    if (current_branch.getDisplayName() == null || current_branch.getDisplayName().isEmpty()) {
      testMessage("Depth [" + depth + "] Branch was empty");
      current_branch = previous_branch;
      return false;
    }
    
    // if the previous and current branch are the same, we just discard the 
    // previous, since the current may have a leaf
    if (current_branch.getDisplayName().equals(previous_branch.getDisplayName())){
      testMessage("Depth [" + depth + "] Current was the same as previous");
      return false;
    }
    
    // we've found a new branch, so add it
    parent_branch.addChild(current_branch);
    testMessage("Depth [" + depth + "] Adding branch: " + current_branch + 
        " to parent: " + parent_branch);
    current_branch = previous_branch;
    return false;
  }
  
  private void parseMetricRule(final Branch parent_branch) {
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

  private void parseTagkRule(final Branch parent_branch) {
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
          rule);
      return;
    }
    
    // matched!
    processParsedValue(parent_branch, tag_name);
    testMessage("Matched tagk [" + rule.getField() + "] for rule: " + 
        rule);
  }
  
  private void parseMetricCustomRule(final Branch parent_branch) {
    if (meta.getMetric() == null) {
      throw new IllegalStateException(
          "Timeseries metric UID object was null");
    }
    Map<String, String> custom = meta.getMetric().getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom metric field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(parent_branch, custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule);
    } else {
      // no match
      testMessage("No match on custom tag [" + rule.getCustomField() 
          + "] for rule: " + rule);
    }
  }
  
  private void parseTagkCustomRule(final Branch parent_branch) {
    if (meta.getTags() == null || meta.getTags().isEmpty()) {
      throw new IllegalStateException(
        "Timeseries meta data was missing tags");
    }
    
    UIDMeta tagk = null;
    for (UIDMeta tag: meta.getTags()) {
      if (tag.getType() == UniqueIdType.TAGK && 
          tag.getName().equals(rule.getField())) {
        tagk = tag;
        break;
      }
    }
    
    if (tagk == null) {
      testMessage("No match on tagk [" + rule.getField() + "] for rule: " + 
          rule);
      return;
    }
    
    testMessage("Matched tagk [" + rule.getField() + "] for rule: " + 
        rule);
    final Map<String, String> custom = tagk.getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom tagk field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(parent_branch, custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
    } else {
      testMessage("No match on custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
      return;
    }
  }
  
  private void parseTagvRule(final Branch parent_branch) {
    if (meta.getTags() == null || meta.getTags().isEmpty()) {
      throw new IllegalStateException(
        "Timeseries meta data was missing tags");
    }
    
    UIDMeta tagv = null;
    for (UIDMeta tag: meta.getTags()) {
      if (tag.getType() == UniqueIdType.TAGV && 
          tag.getName().equals(rule.getField())) {
        tagv = tag;
        break;
      }
    }
    
    if (tagv == null) {
      testMessage("No match on tagv [" + rule.getField() + "] for rule: " + 
          rule);
      return;
    }
    
    testMessage("Matched tagv [" + rule.getField() + "] for rule: " + 
        rule);
    final Map<String, String> custom = tagv.getCustom();
    if (custom != null && custom.containsKey(rule.getCustomField())) {
      if (custom.get(rule.getCustomField()) == null) {
        throw new IllegalStateException(
            "Value for custom tagv field [" + rule.getCustomField() + 
            "] was null");
      }
      processParsedValue(parent_branch, custom.get(rule.getCustomField()));
      testMessage("Matched custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
    } else {
      testMessage("No match on custom tag [" + rule.getCustomField() + 
          "] for rule: " + rule);
      return;
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
          rule);
    }
  }
  
  private void processSplit(final Branch parent_branch, 
      final String parsed_value) {
    if (splits == null) {
      // then this is the first time we're processing the value, so we need to
      // execute the split if there's a separator, after some validation
      if (parsed_value == null || parsed_value.isEmpty()) {
        throw new IllegalArgumentException("Value was empty for rule: " + 
            rule);
      }
      if (rule.getSeparator() == null || rule.getSeparator().isEmpty()) {
        throw new IllegalArgumentException("Separator was empty for rule: " + 
            rule);
      }      
      
      // split it
      splits = parsed_value.split(rule.getSeparator());
      split_idx = 0;
      setCurrentName(parent_branch, parsed_value, splits[split_idx]);
      split_idx++;
    } else {
      // otherwise we have split values and we just need to grab the next one
      setCurrentName(parent_branch, parsed_value, splits[split_idx]);
      split_idx++;
    }
  }
  
  private void processRegexRule(final Branch parent_branch, 
      final String parsed_value) {
    if (rule.getCompiledRegex() == null) {
      throw new IllegalArgumentException("Regex was null for rule: " + 
          rule);
    }

    final Matcher matcher = rule.getCompiledRegex().matcher(parsed_value);
    if (matcher.find()) {
      // The first group is always the full string, so we need to increment
      // by one to fetch the proper group
      if (matcher.groupCount() >= rule.getRegexGroupIdx() + 1) {
        final String extracted = 
          matcher.group(rule.getRegexGroupIdx() + 1);
        if (extracted == null || extracted.isEmpty()) {
          // can't use empty values as a branch/leaf name
          testMessage("Extracted value for rule " + 
              rule + " was null or empty");
        } else {
          // found a branch or leaf!
          setCurrentName(parent_branch, parsed_value, extracted);
        }
      } else {
        // the group index was out of range
        testMessage("Regex group index [" + 
            rule.getRegexGroupIdx() + "] for rule " + 
            rule + " was out of bounds [" +
            matcher.groupCount() + "]");
      }
    }
  }

  /**
   * 
   * @param original_value The original, raw value processed by the calling rule
   * @param extracted_value The post-processed value after the rule worked on it
   */
  private void setCurrentName(final Branch parent_branch, 
      final String original_value, final String extracted_value) {

//    // set the private name first, the one with the separator
//    if (parent_branch == null || parent_branch.getName().isEmpty()) {
//      current_branch.setName(extracted_value);
//    } else {
//      current_branch.setName(parent_branch.getName() + 
//          tree.getNodeSeparator() + extracted_value);
//    }
    
    // now parse and set the display name. If the formatter is empty, we just 
    // set it to the parsed value and exit
    String format = rule.getDisplayFormat();
    if (format == null || format.isEmpty()) {
      current_branch.setDisplayName(extracted_value);
      return;
    }
    
    if (format.contains("{ovalue}")) {
      format = format.replace("{ovalue}", original_value);
    }
    if (format.contains("{value}")) {
      format = format.replace("{value}", extracted_value);
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
        LOG.warn("Display rule " + rule + 
          " was of the wrong type to match on {tag_name}");
        if (test_messages != null) {
          test_messages.add("Display rule " + rule + 
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
   * Resets local state variables to their defaults
   */
  private void resetState() {
    meta = null;
    splits = null;
    rule_idx = 0;
    split_idx = 0;
    current_branch = null;
    rule = null;
    had_nomatch = false;
    root = null;
    test_messages = new ArrayList<String>();
  }

  // GETTERS AND SETTERS --------------------------------
  
  /** @return the local tree object */
  public Tree getTree() {
    return tree;
  }
  
  /** @return the root object */
  public Branch getRootBranch() {
    return root;
  }
//  
//  /** @return the current branch object */
//  public Branch getCurrentBranch() {
//    return current_branch;
//  }
//  
  /** @return the list of test message results */
  public ArrayList<String> getTestMessage() {
    return test_messages;
  }
  
  /** @param The tree to store locally */
  public void setTree(final Tree tree) {
    this.tree = tree;
    calculateMaxLevel();
  }
}

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
import java.util.regex.Pattern;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.stumbleupon.async.Deferred;

/**
 * Represents single rule in a tree set. Used for serialization and CRUD
 * operations
 * @since 2.0
 */
@JsonIgnoreProperties(ignoreUnknown = true) 
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public final class TreeRule {

  /** Types of tree rules */
  public enum TreeRuleType {
    METRIC,         /** A simple metric rule */
    METRIC_CUSTOM,  /** Matches on UID Meta custom field */
    TAGK,           /** Matches on a tagk name */
    TAGK_CUSTOM,    /** Matches on a UID Meta custom field */
    TAGV_CUSTOM     /** Matches on a UID Meta custom field */
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(TreeRule.class);
  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");
  /** ASCII Rule prefix. Qualifier is tree_rule:<level>:<order> */
  private static final byte[] RULE_PREFIX = "tree_rule:".getBytes(CHARSET);
  /** Name of the CF where trees and branches are stored */
  private static final byte[] NAME_FAMILY = "name".getBytes(CHARSET);
  
  /** Type of rule */
  @JsonDeserialize(using = JSON.TreeRuleTypeDeserializer.class)
  private TreeRuleType type = null;
  
  /** Name of the field to match on if applicable */
  private String field = "";
  
  /** Name of the custom field to match on, the key */
  private String custom_field = "";
  
  /** User supplied regular expression before parsing */
  private String regex = "";
  
  /** Separation character or string */
  private String separator = "";
  
  /** An optional description of the rule */
  private String description = "";
  
  /** Optional notes about the rule */
  private String notes = "";
  
  /** Optional group index for extracting from regex matches */
  private int regex_group_idx = 0;
  
  /** Optioanl display format override */
  private String display_format = "";
  
  /** Required level where the rule resides */
  private int level = 0;
  
  /** Required order where the rule resides */
  private int order = 0;
  
  /** The tree this rule belongs to */
  private int tree_id = 0;
  
  /** Compiled regex pattern, compiled after processing */
  private Pattern compiled_regex = null;
  
  /** Tracks fields that have changed by the user to avoid overwrites */
  private final HashMap<String, Boolean> changed = 
    new HashMap<String, Boolean>();
  
  /**
   * Default constructor necessary for de/serialization
   */
  public TreeRule() {
    initializeChangedMap();
  }
  
  /**
   * Constructor initializes the tree ID
   * @param tree_id The tree this rule belongs to
   */
  public TreeRule(final int tree_id) {
    this.tree_id = tree_id;
    initializeChangedMap();
  }
  
  /** TODO - validate rules, i.e. toss errors if they won't work */
  
  /**
   * Copies changed fields from the incoming rule to the local rule
   * @param rule The rule to copy from
   * @param overwrite Whether or not to replace all fields in the local object
   * @return True if there were changes, false if everything was identical
   */
  public boolean copyChanges(final TreeRule rule, final boolean overwrite) {
    if (rule == null) {
      throw new IllegalArgumentException("Cannot copy a null rule");
    }
    if (tree_id != rule.tree_id) {
      throw new IllegalArgumentException("Tree IDs do not match");
    }
    if (level != rule.level) {
      throw new IllegalArgumentException("Levels do not match");
    }
    if (order != rule.order) {
      throw new IllegalArgumentException("Orders do not match");
    }
    
    if (overwrite || (rule.changed.get("type") && type != rule.type)) {
      type = rule.type;
      changed.put("type", true);
    }
    if (overwrite || (rule.changed.get("field") && !field.equals(rule.field))) {
      field = rule.field;
      changed.put("field", true);
    }
    if (overwrite || (rule.changed.get("custom_field") && 
        !custom_field.equals(rule.custom_field))) {
      custom_field = rule.custom_field;
      changed.put("custom_field", true);
    }
    if (overwrite || (rule.changed.get("regex") && !regex.equals(rule.regex))) {
      // validate and compile via the setter
      setRegex(rule.regex);
    }
    if (overwrite || (rule.changed.get("separator") && 
        !separator.equals(rule.separator))) {
      separator = rule.separator;
      changed.put("separator", true);
    }
    if (overwrite || (rule.changed.get("description") &&
        !description.equals(rule.description))) {
      description = rule.description;
      changed.put("description", true);
    }
    if (overwrite || (rule.changed.get("notes") && !notes.equals(rule.notes))) {
      notes = rule.notes;
      changed.put("notes", true);
    }
    if (overwrite || (rule.changed.get("regex_group_idx") && 
        regex_group_idx != rule.regex_group_idx)) {
      regex_group_idx = rule.regex_group_idx;
      changed.put("regex_group_idx", true);
    }
    if (overwrite || (rule.changed.get("display_format") && 
        !display_format.equals(rule.display_format))) {
      display_format = rule.display_format;
      changed.put("display_format", true);
    }
    for (boolean has_changes : changed.values()) {
      if (has_changes) {
        return true;
      }
    }
    return false;
  }
  
  /** @return the rule ID as [tree_id:level:order] */
  @Override
  public String toString() {
    return "[" + tree_id + ":" + level + ":" + order + ":" + type + "]";
  }
  
  public Deferred<Boolean> storeRule(final TSDB tsdb, final boolean overwrite) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    // if there aren't any changes, save time and bandwidth by not writing to
    // storage
    boolean has_changes = false;
    for (Map.Entry<String, Boolean> entry : changed.entrySet()) {
      if (entry.getValue()) {
        has_changes = true;
        break;
      }
    }
    
    if (!has_changes) {
      LOG.trace(this + " does not have changes, skipping sync to storage");
      throw new IllegalStateException("No changes detected in the rule");
    }
    
    // make sure the tree exists, otherwise it's not a good idea to store the
    // rule since it would be orphaned
    if (!Tree.treeExists(tsdb, tree_id)) {
      throw new IllegalArgumentException("Tree: " + tree_id + 
          " could not be found");
    }
    
    TreeRule stored_rule = fetchRule(tsdb, tree_id, level, order);
    final byte[] original_rule = stored_rule == null ? new byte[0] :
      JSON.serializeToBytes(stored_rule);
    if (stored_rule == null) {
      stored_rule = this;
    } else {
      if (!stored_rule.copyChanges(this, overwrite)) {
        LOG.trace(this + " does not have changes, skipping sync to storage");
        throw new IllegalStateException("No changes detected in the rule");
      }
    }
    
    // reset the change map so we don't keep writing
    initializeChangedMap();
    
    // validate before storing
    stored_rule.validateRule();
    
    final PutRequest put = new PutRequest(tsdb.uidTable(), 
        Tree.idToBytes(tree_id), NAME_FAMILY, getQualifier(level, order), 
        JSON.serializeToBytes(stored_rule));
    return tsdb.getClient().compareAndSet(put, original_rule);
  }
  
  public static TreeRule parseFromStorage(final KeyValue column) {
    if (column.value() == null) {
      throw new IllegalArgumentException("Tree rule column value was null");
    }
    
    final TreeRule rule = JSON.parseToObject(column.value(), TreeRule.class);
    rule.initializeChangedMap();
    return rule;
  }
  
  public static TreeRule fetchRule(final TSDB tsdb, final int tree_id, 
      final int level, final int order) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    if (level < 0) {
      throw new IllegalArgumentException("Invalid rule level");
    }
    if (order < 0) {
      throw new IllegalArgumentException("Invalid rule order");
    }
    
    // fetch the whole row
    final GetRequest get = new GetRequest(tsdb.uidTable(), 
        Tree.idToBytes(tree_id));
    get.family(NAME_FAMILY);
    get.qualifier(getQualifier(level, order));
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return null;
      }
      
      return parseFromStorage(row.get(0));
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  public static Deferred<Object> deleteRule(final TSDB tsdb, final int tree_id, 
      final int level, final int order) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    if (level < 0) {
      throw new IllegalArgumentException("Invalid rule level");
    }
    if (order < 0) {
      throw new IllegalArgumentException("Invalid rule order");
    }
    
    final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
        Tree.idToBytes(tree_id), NAME_FAMILY, getQualifier(level, order));
    return tsdb.getClient().delete(delete);
  }
  
  public static void deleteAllRules(final TSDB tsdb, final int tree_id) {
    if (tree_id < 1 || tree_id > 65535) {
      throw new IllegalArgumentException("Invalid Tree ID");
    }
    
    // fetch the whole row
    final GetRequest get = new GetRequest(tsdb.uidTable(), 
        Tree.idToBytes(tree_id));
    get.family(NAME_FAMILY);
    
    try {
      final ArrayList<KeyValue> row = 
        tsdb.getClient().get(get).joinUninterruptibly();
      if (row == null || row.isEmpty()) {
        return;
      }
      
      for (KeyValue column : row) {
        if (column.qualifier().length > RULE_PREFIX.length &&
            Bytes.memcmp(RULE_PREFIX, column.qualifier(), 0, 
            RULE_PREFIX.length) == 0) {
          final DeleteRequest delete = new DeleteRequest(tsdb.uidTable(), 
              Tree.idToBytes(tree_id), NAME_FAMILY, column.qualifier());
          tsdb.getClient().delete(delete);
        }
      }
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Parses a string into a rule type enumerator
   * @param type The string to parse
   * @return The type enumerator
   * @throws IllegalArgumentException if the type was empty or invalid
   */
  public static TreeRuleType stringToType(final String type) {
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Rule type was empty");
    } else if (type.toLowerCase().equals("metric")) {
      return TreeRuleType.METRIC;
    } else if (type.toLowerCase().equals("metric_custom")) {
      return TreeRuleType.METRIC_CUSTOM;
    } else if (type.toLowerCase().equals("tagk")) {
      return TreeRuleType.TAGK;
    } else if (type.toLowerCase().equals("tagk_custom")) {
      return TreeRuleType.TAGK_CUSTOM;
    } else if (type.toLowerCase().equals("tagv_custom")) {
      return TreeRuleType.TAGV_CUSTOM;
    } else {
      throw new IllegalArgumentException("Unrecognized rule type");
    }
  }

  public static byte[] RULE_PREFIX() {
    return RULE_PREFIX;
  }
  
  /**
   * Sets or resets the changed map flags
   */
  private void initializeChangedMap() {
    // set changed flags
    changed.put("type", false);
    changed.put("field", false);
    changed.put("custom_field", false);
    changed.put("regex", false);
    changed.put("separator", false);
    changed.put("description", false);
    changed.put("notes", false);
    changed.put("regex_group_idx", false);
    changed.put("display_format", false);
    changed.put("level", false);
    changed.put("order", false);
    // tree_id can't change
  }
  
  private void validateRule() {
    if (type == null) {
      throw new IllegalArgumentException(
        "Missing rule type");
    }
    
    switch (type) {
      case METRIC:
        // nothing to validate
        break;
      case METRIC_CUSTOM:
      case TAGK_CUSTOM:
      case TAGV_CUSTOM:
        if (field == null || field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing field name required for " + type + " rule");
        }
        if (custom_field == null || custom_field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing custom field name required for " + type + " rule");
        }
        break;
      case TAGK:
        if (field == null || field.isEmpty()) {
          throw new IllegalArgumentException(
              "Missing field name required for " + type + " rule");
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid rule type");
    }
    
    if ((regex != null || !regex.isEmpty()) && regex_group_idx < 0) {
      throw new IllegalArgumentException(
          "Invalid regex group index. Cannot be less than 0");
    }
  }
  
  private static byte[] getQualifier(final int level, final int order) {
    final byte[] suffix = (level + ":" + order).getBytes(CHARSET);
    final byte[] qualifier = new byte[RULE_PREFIX.length + suffix.length];
    System.arraycopy(RULE_PREFIX, 0, qualifier, 0, RULE_PREFIX.length);
    System.arraycopy(suffix, 0, qualifier, RULE_PREFIX.length, suffix.length);
    return qualifier;
  }
  
  // GETTERS AND SETTERS ----------------------------
  
  /** @return the type of rule*/
  public TreeRuleType getType() {
    return type;
  }

  /** @return the name of the field to match on */
  public String getField() {
    return field;
  }

  /** @return the custom_field if matching */
  public String getCustomField() {
    return custom_field;
  }

  /** @return the user supplied, uncompiled regex */
  public String getRegex() {
    return regex;
  }

  /** @return an optional separator*/
  public String getSeparator() {
    return separator;
  }

  /** @return the description of the rule*/
  public String getDescription() {
    return description;
  }

  /** @return the notes */
  public String getNotes() {
    return notes;
  }

  /**  @return the regex_group_idx if using regex group extraction */
  public int getRegexGroupIdx() {
    return regex_group_idx;
  }

  /** @return the display_format */
  public String getDisplayFormat() {
    return display_format;
  }

  /** @return the level where the rule resides*/
  public int getLevel() {
    return level;
  }

  /** @return the order of rule processing within a level */
  public int getOrder() {
    return order;
  }

  /** @return the tree_id */
  public int getTreeId() {
    return tree_id;
  }

  /** @return the compiled_regex */
  @JsonIgnore
  public Pattern getCompiledRegex() {
    return compiled_regex;
  }

  /** @param type The type of rule */
  public void setType(TreeRuleType type) {
    if (this.type != type) {
      changed.put("type", true);
      this.type = type;      
    }
  }

  /** @param field The field name for matching */
  public void setField(String field) {
    if (!this.field.equals(field)) {
      changed.put("field", true);
      this.field = field;
    }
  }

  /** @param custom_field The custom field name to set if matching */
  public void setCustomField(String custom_field) {
    if (!this.custom_field.equals(custom_field)) {
      changed.put("custom_field", true);
      this.custom_field = custom_field;
    }
  }

  /** 
   * @param regex Stores AND compiles the regex string for use in processing
   * @throws PatternSyntaxException if the regex is invalid
   */
  public void setRegex(String regex) {
    if (!this.regex.equals(regex)) {
      changed.put("regex", true);
      this.regex = regex;
      if (regex != null && !regex.isEmpty()) {
        this.compiled_regex = Pattern.compile(regex);
      } else {
        this.compiled_regex = null;
      }
    }
  }

  /** @param separator A character or string to separate on */
  public void setSeparator(String separator) {
    if (!this.separator.equals(separator)) {
      changed.put("separator", true);
      this.separator = separator;
    }
  }

  /** @param description A brief description of the rule */
  public void setDescription(String description) {
    if (!this.description.equals(description)) {
      changed.put("description", true);
      this.description = description;
    }
  }

  /** @param notes Optional detailed notes about the rule */
  public void setNotes(String notes) {
    if (!this.notes.equals(notes)) {
      changed.put("notes", true);
      this.notes = notes;
    }
  }

  /** @param regex_group_idx An optional index (start at 0) to use for regex 
   * group extraction. Must be a positive value. */
  public void setRegexGroupIdx(int regex_group_idx) {
    if (this.regex_group_idx != regex_group_idx) {
      changed.put("regex_group_idx", true);
      this.regex_group_idx = regex_group_idx;
    }
  }

  /** @param display_format Optional format string to alter the display name */
  public void setDisplayFormat(String display_format) {
    if (!this.display_format.equals(display_format)) {
      changed.put("display_format", true);
      this.display_format = display_format;
    }
  }

  /** @param level The top level processing order. Must be 0 or greater 
   * @throws IllegalArgumentException if the level was negative */
  public void setLevel(int level) {
    if (level < 0) {
      throw new IllegalArgumentException("Negative levels are not allowed");
    }
    if (this.level != level) {
      changed.put("level", true);
      this.level = level;
    }
  }

  /** @param order The order of processing within a level. 
   * Must be 0 or greater 
   * @throws IllegalArgumentException if the order was negative */
  public void setOrder(int order) {
    if (level < 0) {
      throw new IllegalArgumentException("Negative orders are not allowed");
    }
    if (this.order != order) {
      changed.put("order", true);
      this.order = order;
    }
  }

  /** @param tree_id The tree_id to set */
  public void setTreeId(int tree_id) {
    this.tree_id = tree_id;
  }  
}

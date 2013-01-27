package net.opentsdb.meta;

import java.util.regex.Pattern;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;

/**
 * Stores information about a single rule for building a tree
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
public class MetaTreeRule {
  private int type;
  private String field;
  private String custom_field;
  private String regex;
  private String separator;
  private String description;
  private String notes;
  private int regex_group_idx;
  private String display_format;
  private int level;
  private int order;
  private int tree_id;
  @JsonIgnore
  private Pattern re;
  
  private boolean c_type;
  private boolean c_field;
  private boolean c_custom_field;
  private boolean c_regex;
  private boolean c_separator;
  private boolean c_description;
  private boolean c_notes;
  private boolean c_regex_group_idx;
  private boolean c_display_format;
  
  /**
   * Default constructor
   */
  public MetaTreeRule(){
    
  }
  
  public MetaTreeRule(final int tree_id, final int level, final int order){
    this.tree_id = tree_id;
    this.level = level;
    this.order = order;
  }
  
  /**
   * Converts an integer into the proper enumerator for tree rule types
   * @return The type of tree rule this rule represents
   */
  @JsonIgnore
  public MetaTree.Tree_Rule_Type ruleType(){
    switch (this.type){
    case 0:
      return MetaTree.Tree_Rule_Type.METRIC;
    case 1:
      return MetaTree.Tree_Rule_Type.METRIC_CUSTOM;
    case 2:
      return MetaTree.Tree_Rule_Type.TAGK;
    case 3:
      return MetaTree.Tree_Rule_Type.TAGK_CUSTOM;
    case 4:
      return MetaTree.Tree_Rule_Type.TAGV_CUSTOM;
    default:
        return MetaTree.Tree_Rule_Type.METRIC;
    }
  }

  public void copyChanges(final MetaTreeRule rule){
    if (rule.c_custom_field)
      this.custom_field = rule.custom_field;
    if (rule.c_description)
      this.description = rule.description;
    if (rule.c_display_format)
      this.display_format = rule.display_format;
    if (rule.c_field)
      this.field = rule.field;
    if (rule.c_notes)
      this.notes = rule.notes;
    if (rule.c_regex){
      this.regex = rule.regex;
      this.re = rule.re;
    }
    if (rule.c_regex_group_idx)
      this.regex_group_idx = rule.regex_group_idx;
    if (rule.c_separator)
      this.separator = rule.separator;
    if (rule.c_type)
      this.type = rule.type;
  }
  
  public String toString(){
    return String.format("[%d:%d] field [%s] type [%d]", level, order, field, type);
  }
  
  public boolean equals(Object obj){
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (obj.getClass() != getClass())
      return false;
    
    MetaTreeRule rule = (MetaTreeRule)obj;
    if (this.type != rule.type)
      return false;
    if (this.field != rule.field)
      return false;
    if (this.custom_field != rule.custom_field)
      return false;
    if (this.regex != rule.regex)
      return false;
    if (this.separator != rule.separator)
      return false;
    if (this.description != rule.description)
      return false;
    if (this.notes != rule.notes)
      return false;
    if (this.regex_group_idx != rule.regex_group_idx)
      return false;
    if (this.display_format != rule.display_format)
      return false;
    if (this.level != rule.level)
      return false;
    if (this.order != rule.order)
      return false;
    if (this.tree_id != rule.tree_id)
      return false;
    
    return true;
  }
  
  public boolean hasChanges(){
    if (this.c_custom_field)
      return true;
    if (this.c_description)
      return true;
    if (this.c_display_format)
      return true;
    if (this.c_field)
      return true;
    if (this.c_notes)
      return true;
    if (this.c_regex)
      return true;
    if (this.c_regex_group_idx)
      return true;
    if (this.c_separator)
      return true;
    if (this.c_type)
      return true;
    return false;
  }
  
  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
    this.c_type = true;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
    this.c_field = true;
  }

  public String getCustom_field() {
    return custom_field;
  }

  public void setCustom_field(String secondary_field) {
    this.custom_field = secondary_field;
    this.c_custom_field = true;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
    this.c_regex = true;
    if (regex != null && !regex.isEmpty())
      re = Pattern.compile(regex);
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
    this.c_separator = true;
  }

  public String getDescription() {
    return this.description;
  }
  
  public void setDescription(String description) {
    this.description = description;
    this.c_description = true;
  }
  
  public String getNotes() {
    return this.notes;
  }
  
  public void setNotes(String notes) {
    this.notes = notes;
    this.c_notes = true;
  }
  
  public int getRegex_group_idx() {
    return regex_group_idx;
  }

  public void setRegex_group_idx(int regex_group_idx) {
    this.regex_group_idx = regex_group_idx;
    this.c_regex_group_idx = true;
  }

  public String getDisplay_format() {
    return display_format;
  }

  public void setDisplay_format(String display_format) {
    this.display_format = display_format;
    this.c_display_format = true;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public int getOrder() {
    return order;
  }

  public void setOrder(int order) {
    this.order = order;
  }

  public int getTree_id() {
    return tree_id;
  }

  public void setTree_id(int tree_id) {
    this.tree_id = tree_id;
  }

  @JsonIgnore
  public Pattern getRe() {
    return re;
  }
}

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
  
  /**
   * Default constructor
   */
  public MetaTreeRule(){
    
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

  public String toString(){
    return String.format("[%d:%d] field [%s] type [%d]", level, order, field, type);
  }
  
  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getCustom_field() {
    return custom_field;
  }

  public void setCustom_field(String secondary_field) {
    this.custom_field = secondary_field;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
    if (regex != null && !regex.isEmpty())
      re = Pattern.compile(regex);
  }

  public String getSeparator() {
    return separator;
  }

  public void setSeparator(String separator) {
    this.separator = separator;
  }

  public String getDescription() {
    return this.description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public String getNotes() {
    return this.notes;
  }
  
  public void setNotes(String notes) {
    this.notes = notes;
  }
  
  public int getRegex_group_idx() {
    return regex_group_idx;
  }

  public void setRegex_group_idx(int regex_group_idx) {
    this.regex_group_idx = regex_group_idx;
  }

  public String getDisplay_format() {
    return display_format;
  }

  public void setDisplay_format(String display_format) {
    this.display_format = display_format;
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

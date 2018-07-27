package net.opentsdb.query.filter;

import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import net.opentsdb.query.filter.TagValueWildcardFilter.Builder;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = TagValueRegexFilter.Builder.class)
public class TagValueRegexFilter extends BaseTagValueFilter {

  /** The compiled pattern */
  final Pattern pattern;
  
  /** Whether or not the regex would match-all. */
  final boolean matches_all;
  
  protected TagValueRegexFilter(final Builder builder) {
    super(builder.tagKey, builder.filter);
    pattern = Pattern.compile(filter);
    
    if (filter.equals(".*") || 
        filter.equals("^.*") || 
        filter.equals(".*$") || 
        filter.equals("^.*$")) {
      // yeah there are many more permutations but these are the most likely
      // to be encountered in the wild.
      matches_all = true;
    } else {
      matches_all = false;
    }
  }

  @Override
  public boolean matches(final Map<String, String> tags) {
    final String tagv = tags.get(tag_key);
    if (tagv == null) {
      return false;
    }
    if (matches_all) {
      return true;
    }
    return pattern.matcher(tagv).find();
  }

  /** Whether or not the regex would match all strings. */
  public boolean matchesAll() {
    return matches_all;
  }
  
  @Override
  public String toString() {
    return new StringBuilder()
        .append("{type=")
        .append(getClass().getSimpleName())
        .append(", tagKey=")
        .append(tag_key)
        .append(", filter=")
        .append(filter)
        .append(", matchesAll=")
        .append(matches_all)
        .append("}")
        .toString();
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder {
    @JsonProperty
    private String tagKey;
    @JsonProperty
    private String filter;
    
    public Builder setTagKey(final String tag_key) {
      this.tagKey = tag_key;
      return this;
    }
    
    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }
    
    public TagValueRegexFilter build() {
      return new TagValueRegexFilter(this);
    }
  }
  
}

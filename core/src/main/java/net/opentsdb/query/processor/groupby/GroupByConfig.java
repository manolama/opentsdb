package net.opentsdb.query.processor.groupby;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.hash.HashCode;

import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;
import net.opentsdb.query.processor.TimeSeriesProcessorConfigBuilder;

@JsonInclude(Include.NON_NULL)
@JsonDeserialize(builder = GroupByConfig.Builder.class)
public class GroupByConfig implements 
  TimeSeriesProcessorConfig<GroupBy>,
  Comparable<GroupByConfig>{

  private TimeSeriesQuery query;

  protected GroupByConfig(final Builder builder) {
    query = builder.query;
  }
  
  public TimeSeriesQuery query() {
    return query;
  }

  /** @return A new builder for the config. */
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final GroupByConfig that = (GroupByConfig) o;

    return Objects.equal(that.query, query);
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    return query.buildHashCode();
  }
  
  @Override
  public int compareTo(final GroupByConfig o) {
    return ComparisonChain.start()
        .compare(query,  o.query)
        .result();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder implements 
      TimeSeriesProcessorConfigBuilder<GroupBy> {
    @JsonProperty
    private TimeSeriesQuery query;
    
    public Builder setQuery(final TimeSeriesQuery query) {
      this.query = query;
      return this;
    }
    
    @Override
    public TimeSeriesProcessorConfig<GroupBy> build() {
      if (query == null) {
        throw new IllegalArgumentException("Query cannot be null.");
      }
      return new GroupByConfig(this);
    }
  }
}

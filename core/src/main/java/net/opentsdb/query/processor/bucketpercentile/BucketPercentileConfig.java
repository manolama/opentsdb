// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.processor.bucketpercentile;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.interpolation.QueryInterpolatorConfig;
import net.opentsdb.query.processor.ratio.RatioConfig;
import net.opentsdb.query.processor.ratio.RatioConfig.Builder;

public class BucketPercentileConfig extends BaseQueryNodeConfigWithInterpolators<
    BucketPercentileConfig.Builder, BucketPercentileConfig> {
  
  public static final String DEFAULT_PATTERN = ".*?[\\.\\-_]([\\-0-9\\.]+)[_\\-]([\\-0-9\\.]+)$";
  
  public static enum OutputOfBucket {
    MEAN,
    TOP,
    BOTTOM
  }
  
  private final String bucket_regex;
  private final Pattern pattern;
  private final double over_flow_max;
  private final String over_flow;
  private final String over_flow_metric;
  private final QueryResultId over_flow_id;
  private final double under_flow_min;
  private final String under_flow;
  private final String under_flow_metric;
  private final QueryResultId under_flow_id;
  private final OutputOfBucket output_of_bucket;
  private final List<String> histograms;
  private final List<String> histogram_metrics;
  private final List<QueryResultId> histogram_ids;
  private final boolean infectious_nan;
  private final String as;
  private final List<Double> percentiles;
  
  private BucketPercentileConfig(final Builder builder) {
    super(builder);
    bucket_regex = Strings.isNullOrEmpty(builder.bucket_regex) ?
        DEFAULT_PATTERN : builder.bucket_regex;
    over_flow = builder.over_flow;
    over_flow_max = builder.over_flow_max;
    over_flow_metric = builder.over_flow_metric;
    over_flow_id = builder.over_flow_id;
    under_flow = builder.under_flow;
    under_flow_min = builder.under_flow_min;
    under_flow_metric = builder.under_flow_metric;
    under_flow_id = builder.under_flow_id;
    output_of_bucket = builder.output_of_bucket == null ? OutputOfBucket.MEAN : builder.output_of_bucket;
    histograms = builder.histograms;
    histogram_metrics = builder.histogram_metrics;
    histogram_ids = builder.histogram_ids;
    infectious_nan = builder.infectiousNan;
    as = builder.as;
    pattern = Pattern.compile(bucket_regex);
    
    if (builder.percentiles == null || builder.percentiles.isEmpty()) {
      throw new IllegalArgumentException("Percentiles cannot be null or empty.");
    }
    
    // We want to convert 99.9 to 0.999
    boolean convert = false;
    for (int i = 0; i < builder.percentiles.size(); i++) {
      if (builder.percentiles.get(i) > 1) {
        convert = true;
        break;
      }
    }
    
    if (convert) {
      percentiles = Lists.newArrayList();
      for (int i = 0; i < builder.percentiles.size(); i++) {
        percentiles.add(builder.percentiles.get(i) / 100);
      }
    } else {
      percentiles = builder.percentiles;
    }
    Collections.sort(percentiles);
    
    result_ids = Lists.newArrayList(new DefaultQueryResultId(as, as));
  }
  
  public String getBucketRegex() {
    return bucket_regex;
  }
  
  public Pattern pattern() {
    return pattern;
  }
  
  public String getOverFlow() {
    return over_flow;
  }
  
  public double getOverFlowMax() {
    return over_flow_max;
  }
  
  public String overFlowMetric() {
    return over_flow_metric;
  }
  
  public QueryResultId overFlowId() {
    return over_flow_id;
  }
  
  public String getUnderFlow() {
    return under_flow;
  }
  
  public double getUnderFlowMin() {
    return under_flow_min;
  }
  
  public String underFlowMetric() {
    return under_flow_metric;
  }
  
  public QueryResultId underFlowId() {
    return under_flow_id;
  }
  
  public OutputOfBucket getOutputOfBucket() {
    return output_of_bucket;
  }
  
  public List<String> getHistograms() {
    return histograms;
  }
  
  /** @return The list of data sources to work on. */
  public List<String> histogramMetrics() {
    return histogram_metrics;
  }
  
  public List<QueryResultId> histogramIds() {
    return histogram_ids;
  }
  
  /** @return The metric name to use for the ratios. */
  public String getAs() {
    return as;
  }
  
  public List<Double> getPercentiles() {
    return percentiles;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  @Override
  public boolean pushDown() {
    return true;
  }

  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setAs(as)
        .setOutputOfBucket(output_of_bucket)
        .setBucketRegex(bucket_regex)
        .setOverFlow(over_flow)
        .setOverFlowMetric(over_flow_metric)
        .setOverFlowId(over_flow_id)
        .setUnderFlow(under_flow)
        .setUnderFlowMetric(under_flow_metric)
        .setUnderFlowId(under_flow_id)
        .setHistograms(histograms)
        .setHistogramMetrics(histogram_metrics)
        .setHistogramIds(histogram_ids)
        .setInfectiousNan(infectious_nan)
        .setPercentiles(percentiles);
    super.toBuilder(builder);
    return builder;
  }
  
  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BucketPercentileConfig config = (BucketPercentileConfig) o;
    return Objects.equal(as, config.getAs()) &&
        // TODO!
        Objects.equal(histograms, config.getHistograms()) &&
        Objects.equal(infectious_nan, config.getInfectiousNan()) &&
        Objects.equal(interpolator_configs, config.interpolator_configs) &&
        Objects.equal(id, config.getId());
  }
  
  @Override
  public int hashCode() {
    return buildHashCode().asInt();
  }
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode() {
    final List<HashCode> hashes =
        Lists.newArrayListWithCapacity(3);
    hashes.add(super.buildHashCode());
    
    if (interpolator_configs != null && 
        !interpolator_configs.isEmpty()) {
      final Map<String, QueryInterpolatorConfig> sorted = 
          new TreeMap<String, QueryInterpolatorConfig>();
      for (final Entry<TypeToken<?>, QueryInterpolatorConfig> entry : 
          interpolator_configs.entrySet()) {
        sorted.put(entry.getKey().toString(), entry.getValue());
      }
      for (final Entry<String, QueryInterpolatorConfig> entry : sorted.entrySet()) {
        hashes.add(entry.getValue().buildHashCode());
      }
    }
    
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    for (int i = 0; i < histograms.size(); i++) {
      hasher.putString(histograms.get(i), Const.UTF8_CHARSET);
    }
    hasher.putString(as, Const.UTF8_CHARSET)
          .putBoolean(infectious_nan);
    hashes.add(hasher.hash());
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final BucketPercentileConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  /**
   * Parses a JSON config.
   * @param mapper The non-null mapper.
   * @param tsdb The non-null TSDB for factories.
   * @param node The non-null node.
   * @return The parsed config.
   */
  public static BucketPercentileConfig parse(final ObjectMapper mapper,
                                  final TSDB tsdb,
                                  final JsonNode node) {
    System.out.println("BUILDER!!!!!!!!!!!!!!!!!!!!");
    Builder builder = new Builder();
    JsonNode n = node.get("histograms");
    if (n != null && !n.isNull()) {
      List<String> sources = Lists.newArrayList();
      for (final JsonNode source : n) {
        sources.add(source.asText());
      }
      builder.setHistograms(sources);
    }
    
    n = node.get("percentiles");
    if (n != null && !n.isNull()) {
      for (final JsonNode ptile : n) {
        builder.addPercentile(ptile.asDouble());
      }
    }
    
    n = node.get("bucketRegex");
    if (n != null && !n.isNull()) {
      builder.setBucketRegex(n.asText());
    }
    
    n = node.get("overFlow");
    if (n != null && !n.isNull()) {
      builder.setOverFlow(n.asText());
    }
    
    n = node.get("underFlow");
    if (n != null && !n.isNull()) {
      builder.setUnderFlow(n.asText());
    }
    
    n = node.get("outputOfBucket");
    if (n != null && !n.isNull()) {
      builder.setOutputOfBucket(OutputOfBucket.valueOf(n.asText()));
    }
    
    n = node.get("as");
    if (n != null && !n.isNull()) {
      builder.setAs(n.asText());
    }
    
    BaseQueryNodeConfigWithInterpolators.parse(builder, mapper, tsdb, node);
    
    return builder.build();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Builder extends BaseQueryNodeConfigWithInterpolators.Builder
      <Builder, BucketPercentileConfig> {
    private String bucket_regex;
    private String over_flow;
    private double over_flow_max;
    private String over_flow_metric;
    private QueryResultId over_flow_id;
    private String under_flow;
    private double under_flow_min;
    private String under_flow_metric;
    private QueryResultId under_flow_id;
    private OutputOfBucket output_of_bucket;
    private List<String> histograms;
    private List<String> histogram_metrics;
    private List<QueryResultId> histogram_ids;
    private List<Double> percentiles;
    private boolean infectiousNan;
    private String as;
    
    Builder() {
      setType(BucketPercentileFactory.TYPE);
    }
    
    public QueryResultId overFlowId() {
      return over_flow_id;
    }
    
    public QueryResultId underFlowId() {
      return under_flow_id;
    }
    
    public List<QueryResultId> histogramIds() {
      return histogram_ids;
    }
    
    public Builder setHistograms(final List<String> histograms) {
      this.histograms = histograms;
      return this;
    }
    
    public Builder addHistogram(final String histogram) {
      if (histograms == null) {
        histograms = Lists.newArrayList();
      }
      histograms.add(histogram);
      return this;
    }
    
    public Builder setHistogramMetrics(final List<String> histogram_metrics) {
      this.histogram_metrics = histogram_metrics;
      return this;
    }
    
    public Builder addHistogramMetric(final String histogram_metric) {
      if (histogram_metrics == null) {
        histogram_metrics = Lists.newArrayList();
      }
      histogram_metrics.add(histogram_metric);
      return this;
    }
    
    public Builder setHistogramIds(final List<QueryResultId> histogram_ids) {
      this.histogram_ids = histogram_ids;
      return this;
    }
    
    public Builder addHistogramId(final QueryResultId histogram_id) {
      if (histogram_ids == null) {
        histogram_ids = Lists.newArrayList();
      }
      histogram_ids.add(histogram_id);
      return this;
    }
    
    public Builder setAs(final String as) {
      this.as = as;
      return this;
    }
    
    public Builder setBucketRegex(final String bucket_regex) {
      this.bucket_regex = bucket_regex;
      return this;
    }
    
    public Builder setOverFlow(final String over_flow) {
      this.over_flow = over_flow;
      return this;
    }
    
    public Builder setOverFlowMetric(final String over_flow_metric) {
      this.over_flow_metric = over_flow_metric;
      return this;
    }
    
    public Builder setOverFlowId(final QueryResultId over_flow_id) {
      this.over_flow_id = over_flow_id;
      return this;
    }
    
    public Builder setUnderFlow(final String under_flow) {
      this.under_flow = under_flow;
      return this;
    }
    
    public Builder setUnderFlowMetric(final String under_flow_metric) {
      this.under_flow_metric = under_flow_metric;
      return this;
    }
    
    public Builder setUnderFlowId(final QueryResultId under_flow_id) {
      this.under_flow_id = under_flow_id;
      return this;
    }
    
    public Builder setOutputOfBucket(final OutputOfBucket output_of_bucket) {
      this.output_of_bucket = output_of_bucket;
      return this;
    }
    
    /**
     * @param infectious_nan Whether or not NaNs should be sentinels or included
     * in arithmetic.
     * @return The builder.
     */
    public Builder setInfectiousNan(final boolean infectious_nan) {
      this.infectiousNan = infectious_nan;
      return this;
    }
    
    public Builder setPercentiles(final List<Double> percentiles) {
      this.percentiles = percentiles;
      return this;
    }
    
    public Builder addPercentile(final double percentile) {
      if (percentiles == null) {
        percentiles = Lists.newArrayList();
      }
      percentiles.add(percentile);
      return this;
    }
    
    public BucketPercentileConfig build() {
      return new BucketPercentileConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
    
  }
}

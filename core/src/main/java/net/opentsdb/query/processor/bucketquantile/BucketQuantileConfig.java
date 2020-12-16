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
package net.opentsdb.query.processor.bucketquantile;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.BaseQueryNodeConfigWithInterpolators;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryResultId;

/**
 * A complex config class for the bucket quantile node since there are a lot of
 * tweaks folks can make. We'll try to choose useful defaults.
 * 
 * @since 3.0
 */
public class BucketQuantileConfig extends BaseQueryNodeConfigWithInterpolators<
    BucketQuantileConfig.Builder, BucketQuantileConfig> {
  public static final String DEFAULT_PATTERN = 
      ".*?[\\.\\-_](\\-?[0-9\\.]+[eE]?\\-?[0-9]*)[_\\-](\\-?[0-9\\.]+[eE]?\\-?[0-9]*)$";
  
  /**
   * Determines the output of the bucket value for the bounded histogram buckets.
   * E.g. the default of mean returns the mean of the upper and lower bounds while
   * top takes the upper and bottom the lower.
   */
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
  private final List<Double> quantiles;
  private final boolean cumulative_buckets;
  private final boolean counter_buckets;
  private final double nan_threshold;
  private final double missing_metric_threshold;
  
  private BucketQuantileConfig(final Builder builder) {
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
    output_of_bucket = builder.output_of_bucket == null ? 
        OutputOfBucket.MEAN : builder.output_of_bucket;
    histograms = builder.histograms;
    histogram_metrics = builder.histogram_metrics;
    histogram_ids = builder.histogram_ids;
    infectious_nan = builder.infectiousNan;
    as = builder.as;
    pattern = Pattern.compile(bucket_regex);
    cumulative_buckets = builder.cumulative_buckets;
    counter_buckets = builder.counter_buckets;
    nan_threshold = builder.nan_threshold;
    missing_metric_threshold = builder.missing_metric_threshold;
    
    if (Strings.isNullOrEmpty(as)) {
      throw new IllegalArgumentException("As cannot be null or empty.");
    }
    if (builder.quantiles == null || builder.quantiles.isEmpty()) {
      throw new IllegalArgumentException("Percentiles cannot be null or empty.");
    }
    if (histograms == null || histograms.isEmpty()) {
      throw new IllegalArgumentException("Histograms cannot be empty.");
    }
    
    // We want to convert 99.9 to 0.999
    boolean convert = false;
    for (int i = 0; i < builder.quantiles.size(); i++) {
      if (builder.quantiles.get(i) > 1) {
        convert = true;
        // NOTE We assume all quantiles are formatted the same.
        break;
      }
    }
    
    if (convert) {
      quantiles = Lists.newArrayList();
      for (int i = 0; i < builder.quantiles.size(); i++) {
        quantiles.add(builder.quantiles.get(i) / 100);
      }
    } else {
      quantiles = builder.quantiles;
    }
    Collections.sort(quantiles);
    Collections.sort(histograms);
    
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
  
  public List<Double> getQuantiles() {
    return quantiles;
  }
  
  public boolean getCumulativeBuckets() {
    return cumulative_buckets;
  }
  
  public boolean getCounterBuckets() {
    return counter_buckets;
  }

  public double getNanThreshold() {
    return nan_threshold;
  }
  
  public double getMissingMetricThreshold() {
    return missing_metric_threshold;
  }
  
  /** @return Whether or not NaNs should be treated as sentinels or considered 
   * in arithmetic. */
  public boolean getInfectiousNan() {
    return infectious_nan;
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return true;
  }

  @Override
  public Builder toBuilder() {
    final Builder builder = new Builder()
        .setAs(as)
        .setBucketRegex(bucket_regex)
        .setOverFlowMax(over_flow_max)
        .setOverFlow(over_flow)
        .setOverFlowMetric(over_flow_metric)
        .setUnderFlowMin(under_flow_min)
        .setOverFlowId(over_flow_id)
        .setUnderFlow(under_flow)
        .setUnderFlowMetric(under_flow_metric)
        .setUnderFlowId(under_flow_id)
        .setOutputOfBucket(output_of_bucket)
        .setHistograms(Lists.newArrayList(histograms))
        .setHistogramMetrics(histogram_metrics == null ? null : 
            Lists.newArrayList(histogram_metrics))
        .setHistogramIds(histogram_ids == null ? null : 
            Lists.newArrayList(histogram_ids))
        .setInfectiousNan(infectious_nan)
        .setQuantiles(Lists.newArrayList(quantiles))
        .setCumulativeBuckets(cumulative_buckets)
        .setCounterBuckets(counter_buckets)
        .setNanThreshold(nan_threshold)
        .setMissingMetricThreshold(missing_metric_threshold);
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
    final BucketQuantileConfig config = (BucketQuantileConfig) o;
    return Objects.equal(as, config.getAs()) &&
        Objects.equal(bucket_regex, config.bucket_regex) &&
        Objects.equal(over_flow, config.over_flow) &&
        Objects.equal(over_flow_max, config.over_flow_max) &&
        Objects.equal(under_flow, config.under_flow) &&
        Objects.equal(under_flow_min, config.under_flow_min) &&
        Objects.equal(output_of_bucket, config.output_of_bucket) &&
        Objects.equal(histograms, config.getHistograms()) &&
        Objects.equal(quantiles, config.quantiles) &&
        Objects.equal(cumulative_buckets, config.cumulative_buckets) &&
        Objects.equal(counter_buckets, config.counter_buckets) &&
        Objects.equal(nan_threshold, config.nan_threshold) &&
        Objects.equal(missing_metric_threshold, config.missing_metric_threshold) &&
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
        
    final Hasher hasher = Const.HASH_FUNCTION().newHasher();
    hasher.putString(bucket_regex, Const.UTF8_CHARSET)
          .putDouble(over_flow_max)
          .putString(over_flow == null ? "" : over_flow, Const.UTF8_CHARSET)
          .putDouble(under_flow_min)
          .putString(under_flow == null ? "" : under_flow, Const.UTF8_CHARSET)
          .putInt(output_of_bucket.ordinal())
          .putString(as, Const.UTF8_CHARSET)
          .putBoolean(cumulative_buckets)
          .putBoolean(counter_buckets)
          .putDouble(nan_threshold)
          .putDouble(missing_metric_threshold);
    for (int i = 0; i < histograms.size(); i++) {
      hasher.putString(histograms.get(i), Const.UTF8_CHARSET);
    }
    for (int i = 0; i < quantiles.size(); i++) {
      hasher.putDouble(quantiles.get(i));
    }
    hasher.putString(as, Const.UTF8_CHARSET)
          .putBoolean(infectious_nan);
    hashes.add(hasher.hash());
    return Hashing.combineOrdered(hashes);
  }

  @Override
  public int compareTo(final BucketQuantileConfig o) {
    // TODO Auto-generated method stub
    return 0;
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  /**
   * Parses a JSON config.
   * @param mapper The non-null mapper.
   * @param tsdb The non-null TSDB for factories.
   * @param node The non-null node.
   * @return The parsed config.
   */
  public static BucketQuantileConfig parse(final ObjectMapper mapper,
                                  final TSDB tsdb,
                                  final JsonNode node) {
    Builder builder = new Builder();
    JsonNode n = node.get("histograms");
    if (n != null && !n.isNull()) {
      List<String> sources = Lists.newArrayList();
      for (final JsonNode source : n) {
        sources.add(source.asText());
      }
      builder.setHistograms(sources);
    }
    
    n = node.get("quantiles");
    if (n != null && !n.isNull()) {
      for (final JsonNode ptile : n) {
        builder.addQuantile(ptile.asDouble());
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
    
    n = node.get("overFlowMax");
    if (n != null && !n.isNull()) {
      builder.setOverFlowMax(n.asDouble());
    }
    
    n = node.get("underFlow");
    if (n != null && !n.isNull()) {
      builder.setUnderFlow(n.asText());
    }
    
    n = node.get("underFlowMin");
    if (n != null && !n.isNull()) {
      builder.setUnderFlowMin(n.asDouble());
    }
    
    n = node.get("outputOfBucket");
    if (n != null && !n.isNull()) {
      builder.setOutputOfBucket(OutputOfBucket.valueOf(n.asText()));
    }
    
    n = node.get("as");
    if (n != null && !n.isNull()) {
      builder.setAs(n.asText());
    }
    
    n = node.get("cumulativeBuckets");
    if (n != null && !n.isNull()) {
      builder.setCumulativeBuckets(n.asBoolean());
    }
    
    n = node.get("counterBuckets");
    if (n != null && !n.isNull()) {
      builder.setCounterBuckets(n.asBoolean());
    }
    
    n = node.get("nanThreshold");
    if (n != null && !n.isNull()) {
      builder.setNanThreshold(n.asDouble());
    }
    
    n = node.get("missingMetricThreshold");
    if (n != null && !n.isNull()) {
      builder.setMissingMetricThreshold(n.asDouble());
    }
    
    n = node.get("infectiousNan");
    if (n != null && !n.isNull()) {
      builder.setInfectiousNan(n.asBoolean());
    }
    
    BaseQueryNodeConfigWithInterpolators.parse(builder, mapper, tsdb, node);
    
    return builder.build();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Builder extends BaseQueryNodeConfigWithInterpolators.Builder
      <Builder, BucketQuantileConfig> {
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
    private List<Double> quantiles;
    private boolean cumulative_buckets;
    private boolean counter_buckets;
    private double nan_threshold;
    private double missing_metric_threshold;
    private boolean infectiousNan;
    private String as;
    
    Builder() {
      setType(BucketQuantileFactory.TYPE);
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
    
    public Builder setOverFlowMax(final double over_flow_max) {
      this.over_flow_max = over_flow_max;
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
    
    public Builder setUnderFlowMin(final double under_flow_min) {
      this.under_flow_min = under_flow_min;
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
    
    public Builder setQuantiles(final List<Double> quantiles) {
      this.quantiles = quantiles;
      return this;
    }
    
    public Builder addQuantile(final double quantile) {
      if (quantiles == null) {
        quantiles = Lists.newArrayList();
      }
      quantiles.add(quantile);
      return this;
    }
    
    public Builder setCumulativeBuckets(final boolean cumulative_buckets) {
      this.cumulative_buckets = cumulative_buckets;
      return this;
    }
    
    public Builder setCounterBuckets(final boolean counter_buckets) {
      this.counter_buckets = counter_buckets;
      return this;
    }
    
    public Builder setNanThreshold(final double nan_threshold) {
      this.nan_threshold = nan_threshold;
      return this;
    }
    
    public Builder setMissingMetricThreshold(final double missing_metric_threshold) {
      this.missing_metric_threshold = missing_metric_threshold;
      return this;
    }
    
    public BucketQuantileConfig build() {
      return new BucketQuantileConfig(this);
    }

    @Override
    public Builder self() {
      return this;
    }
    
  }
}

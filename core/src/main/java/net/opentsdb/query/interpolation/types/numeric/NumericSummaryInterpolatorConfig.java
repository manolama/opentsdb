package net.opentsdb.query.interpolation.types.numeric;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.types.numeric.BaseNumericFillPolicy;
import net.opentsdb.data.types.numeric.BaseNumericSummaryFillPolicy;
import net.opentsdb.data.types.numeric.NumericAggregator;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryIteratorInterpolator;
import net.opentsdb.query.QueryInterpolatorConfig;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.groupby.GroupByConfig.Builder;
import net.opentsdb.rollup.RollupConfig;

public class NumericSummaryInterpolatorConfig implements QueryInterpolatorConfig {

  /** The default numeric fill policy. */
  protected final FillPolicy fill_policy;
  
  /** The default real value fill policy. */
  protected final FillWithRealPolicy real_fill;
  
  protected final Map<Integer, FillPolicy> summary_fill_policy_overrides;
  
  protected final Map<Integer, FillWithRealPolicy> summary_real_fill_overrides;
  
  protected final boolean sync;
  
  protected final List<Integer> expected_summaries;
  
  protected final NumericAggregator component_agg;
  private final RollupConfig rollup_config;
  
  NumericSummaryInterpolatorConfig(final Builder builder) {
    if (builder.expected_summaries == null || builder.expected_summaries.isEmpty()) {
      throw new IllegalArgumentException("Expected summaries cannot be null or empty.");
    }
    fill_policy = builder.fill_policy;
    real_fill = builder.real_fill;
    summary_fill_policy_overrides = builder.summary_fill_policy_overrides;
    summary_real_fill_overrides = builder.summary_real_fill_overrides;
    sync = builder.sync;
    expected_summaries = builder.expected_summaries;
    component_agg = builder.component_agg;
    rollup_config = builder.rollup_config;
  }
  
  /** @return The default numeric fill policy. */
  public FillPolicy defaultFillPolicy() {
    return fill_policy;
  }
  
  /** @return The default real fill policy. */
  public FillWithRealPolicy defaultRealFillPolicy() {
    return real_fill;
  }
  
  /**
   * 
   * @param summary
   * @return The override or the default.
   */
  public FillPolicy fillPolicy(final int summary) {
    final FillPolicy policy = summary_fill_policy_overrides == null ? null : summary_fill_policy_overrides.get(summary);
    if (policy == null) {
      return fill_policy;
    }
    return policy;
  }
  
  /**
   * 
   * @param summary
   * @return The override or default
   */
  public FillWithRealPolicy realFillPolicy(final int summary) {
    FillWithRealPolicy policy = summary_real_fill_overrides == null ? null : summary_real_fill_overrides.get(summary);
    if (policy == null) {
      return real_fill;
    }
    return policy;
  }
  
  public QueryFillPolicy<NumericType> queryFill(final int summary) {
    final NumericInterpolatorConfig config = NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(fillPolicy(summary))
        .setRealFillPolicy(realFillPolicy(summary))
        .build();
    return new BaseNumericFillPolicy(config);
  }
  
  public boolean sync() {
    return sync;
  }
  
  public List<Integer> expectedSummaries() {
    return expected_summaries;
  }
  
  public NumericAggregator componentAggregator() {
    return component_agg;
  }
  
  /** @return The base numeric fill using the {@link #fillPolicy()}. */
  public QueryFillPolicy<NumericSummaryType> queryFill() {
    return new BaseNumericSummaryFillPolicy(this);
  }
  
  public RollupConfig rollupConfig() {
    return rollup_config;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private FillPolicy fill_policy;
    private FillWithRealPolicy real_fill;
    private Map<Integer, FillPolicy> summary_fill_policy_overrides;
    private Map<Integer, FillWithRealPolicy> summary_real_fill_overrides;
    private boolean sync;
    private List<Integer> expected_summaries;
    private NumericAggregator component_agg;
    private RollupConfig rollup_config;
    
    /**
     * @param fill_policy A non-null numeric fill policy.
     * @return The builder.
     */
    public Builder setDefaultFillPolicy(final FillPolicy fill_policy) {
      this.fill_policy = fill_policy;
      return this;
    }
    
    /**
     * @param real_fill A non-null real fill policy.
     * @return The builder.
     */
    public Builder setDefaultRealFillPolicy(final FillWithRealPolicy real_fill) {
      this.real_fill = real_fill;
      return this;
    }
    
    public Builder setFillPolicyOverrides(final Map<Integer, FillPolicy> summary_fill_policy_overrides) {
      this.summary_fill_policy_overrides = summary_fill_policy_overrides;
      return this;
    }
    
    public Builder setRealFillPolicyOverrides(final Map<Integer, FillWithRealPolicy> summary_real_fill_overrides) {
      this.summary_real_fill_overrides = summary_real_fill_overrides;
      return this;
    }
    
    public Builder addFillPolicyOverride(final int summary, final FillPolicy fill_policy) {
      if (summary_fill_policy_overrides == null) {
        summary_fill_policy_overrides = Maps.newHashMapWithExpectedSize(1);
      }
      summary_fill_policy_overrides.put(summary, fill_policy);
      return this;
    }
    
    public Builder addRealFillPolicyOverride(final int summary, final FillWithRealPolicy fill_policy) {
      if (summary_real_fill_overrides == null) {
        summary_real_fill_overrides = Maps.newHashMapWithExpectedSize(1);
      }
      summary_real_fill_overrides.put(summary, fill_policy);
      return this;
    }
    
    public Builder setSync(final boolean sync) {
      this.sync = sync;
      return this;
    }
    
    public Builder setExpectedSummaries(final List<Integer> expected_summaries) {
      this.expected_summaries = expected_summaries;
      return this;
    }
    
    public Builder addExpectedSummary(final int summary) {
      if (expected_summaries == null) {
        expected_summaries = Lists.newArrayListWithExpectedSize(1);
      }
      expected_summaries.add(summary);
      return this;
    }
    
    public Builder setComponentAggregator(final NumericAggregator component_agg) {
      this.component_agg = component_agg;
      return this;
    }
    
    public Builder setRollupConfig(final RollupConfig rollup_config) {
      this.rollup_config = rollup_config;
      return this;
    }
    
    /** @return An instantiated interpolator config. */
    public NumericSummaryInterpolatorConfig build() {
      return new NumericSummaryInterpolatorConfig(this);
    }
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.query.egads.olympicscoring;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.anomaly.BaseAnomalyConfig;

/**
 * TODO ------- SUPER IMPORTANT!!! EQUALS AND HASH
 *
 */
public class OlympicScoringConfig extends BaseAnomalyConfig {
  
  private final SemanticQuery baseline_query;
  private final String baseline_period;
  private final int baseline_num_periods;
  private final String baseline_aggregator;
  private final int exclude_max;
  private final int exclude_min;
  private final double upper_threshold_bad;
  private final double upper_threshold_warn;
  private final boolean upper_is_scalar;
  private final double lower_threshold_bad;
  private final double lower_threshold_warn;
  private final boolean lower_is_scalar;
  
  protected OlympicScoringConfig(final Builder builder) {
    super(builder);
    baseline_query = builder.baselineQuery;
    baseline_period = builder.baselinePeriod;
    baseline_num_periods = builder.baselineNumPeriods;
    baseline_aggregator = builder.baselineAggregator;
    exclude_max = builder.excludeMax;
    exclude_min = builder.excludeMin;
    upper_threshold_bad = builder.upperThresholdBad;
    upper_threshold_warn = builder.upperThresholdWarn;
    upper_is_scalar = builder.upperIsScalar;
    lower_threshold_bad = builder.lowerThresholdBad;
    lower_threshold_warn = builder.lowerThresholdWarn;
    lower_is_scalar = builder.lowerIsScalar;
  }
  
  public SemanticQuery getBaselineQuery() {
    return baseline_query;
  }

  public String getBaselinePeriod() {
    return baseline_period;
  }
  
  public int getBaselineNumPeriods() {
    return baseline_num_periods;
  }

  public String getBaselineAggregator() {
    return baseline_aggregator;
  }
  
  public int getExcludeMax() {
    return exclude_max;
  }

  public int getExcludeMin() {
    return exclude_min;
  }

  public double getUpperThresholdBad() {
    return upper_threshold_bad;
  }
  
  public double getUpperThresholdWarn() {
    return upper_threshold_warn;
  }

  public boolean isUpperIsScalar() {
    return upper_is_scalar;
  }

  public double getLowerThresholdBad() {
    return lower_threshold_bad;
  }
  
  public double getLowerThresholdWarn() {
    return lower_threshold_warn;
  }

  public boolean isLowerIsScalar() {
    return lower_is_scalar;
  }

  @Override
  public boolean pushDown() {
    return false;
  }

  @Override
  public boolean joins() {
    return false;
  }

  @Override
  public Builder toBuilder() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Builder extends BaseAnomalyConfig.Builder<
      Builder, OlympicScoringConfig> {
    @JsonProperty
    private SemanticQuery baselineQuery;
    @JsonProperty
    private String baselinePeriod;
    @JsonProperty
    private int baselineNumPeriods;
    @JsonProperty
    private String baselineAggregator;
    @JsonProperty
    private int excludeMax;
    @JsonProperty
    private int excludeMin;
    @JsonProperty
    private double upperThresholdBad;
    @JsonProperty
    private double upperThresholdWarn;
    @JsonProperty
    private boolean upperIsScalar;
    @JsonProperty
    private double lowerThresholdBad;
    @JsonProperty
    private double lowerThresholdWarn;
    @JsonProperty
    private boolean lowerIsScalar;
    
    Builder() {
      setType(OlympicScoringFactory.TYPE);
    }
    
    public Builder setBaselineQuery(final SemanticQuery baseline_query) {
      baselineQuery = baseline_query;
      return this;
    }
    
    public Builder setBaselinePeriod(final String baseline_period) {
      baselinePeriod = baseline_period;
      return this;
    }
    
    public Builder setBaselineNumPeriods(final int baseline_num_periods) {
      baselineNumPeriods = baseline_num_periods;
      return this;
    }
    
    public Builder setBaselineAggregator(final String baseline_aggregator) {
      baselineAggregator = baseline_aggregator;
      return this;
    }
    
    public Builder setExcludeMax(final int exclude_max) {
      excludeMax = exclude_max;
      return this;
    }
    
    public Builder setExcludeMin(final int exclude_min) {
      excludeMin = exclude_min;
      return this;
    }
    
    public Builder setUpperThresholdBad(final double upper_threshold) {
      upperThresholdBad = upper_threshold;
      return this;
    }
    
    public Builder setUpperThresholdWarn(final double upper_threshold) {
      upperThresholdWarn = upper_threshold;
      return this;
    }
    
    public Builder setUpperIsScalar(final boolean upper_is_scalar) {
      upperIsScalar = upper_is_scalar;
      return this;
    }
    
    public Builder setLowerThresholdBad(final double lower_treshold) {
      lowerThresholdBad = lower_treshold;
      return this;
    }
    
    public Builder setLowerThresholdWarn(final double lower_treshold) {
      lowerThresholdWarn = lower_treshold;
      return this;
    }
    
    public Builder setLowerIsScalar(final boolean lower_is_scalar) {
      lowerIsScalar = lower_is_scalar;
      return this;
    }
    
    @Override
    public Builder self() {
      return this;
    }

    @Override
    public OlympicScoringConfig build() {
      return new OlympicScoringConfig(this);
    }
    
  }
}

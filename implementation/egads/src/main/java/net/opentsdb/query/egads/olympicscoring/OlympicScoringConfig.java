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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.anomaly.BaseAnomalyConfig;

public class OlympicScoringConfig extends BaseAnomalyConfig {
  
  private final SemanticQuery baseline_query;
  private final String baseline_period;
  private final int baseline_num_periods;
  private final String baseline_aggregator;
  private final int exclude_max;
  private final int exclude_min;
  private final double upper_threshold;
  private final boolean upper_is_scalar;
  private final double lower_threshold;
  private final boolean lower_is_scalar;
  
  protected OlympicScoringConfig(final Builder builder) {
    super(builder);
    baseline_query = builder.baselineQuery;
    baseline_period = builder.baselinePeriod;
    baseline_num_periods = builder.baselineNumPeriods;
    baseline_aggregator = builder.baselineAggregator;
    exclude_max = builder.excludeMax;
    exclude_min = builder.excludeMin;
    upper_threshold = builder.upperThreshold;
    upper_is_scalar = builder.upperIsScalar;
    lower_threshold = builder.lowerThreshold;
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

  public double getUpperThreshold() {
    return upper_threshold;
  }

  public boolean isUpperIsScalar() {
    return upper_is_scalar;
  }

  public double getLowerThreshold() {
    return lower_threshold;
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
  public static class Builder extends BaseAnomalyConfig.Builder<Builder, OlympicScoringConfig> {
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
    private double upperThreshold;
    @JsonProperty
    private boolean upperIsScalar;
    @JsonProperty
    private double lowerThreshold;
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
    
    public Builder setUpperThreshold(final double upper_threshold) {
      upperThreshold = upper_threshold;
      return this;
    }
    
    public Builder setUpperIsScalar(final boolean upper_is_scalar) {
      upperIsScalar = upper_is_scalar;
      return this;
    }
    
    public Builder setLowerThreshold(final double lower_treshold) {
      lowerThreshold = lower_treshold;
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

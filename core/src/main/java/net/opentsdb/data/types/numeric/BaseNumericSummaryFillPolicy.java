package net.opentsdb.data.types.numeric;

import java.util.Collection;

import net.opentsdb.query.QueryFillPolicy;
import net.opentsdb.query.QueryInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class BaseNumericSummaryFillPolicy implements QueryFillPolicy<NumericSummaryType>, 
  NumericSummaryType {

  protected final NumericSummaryInterpolatorConfig config;
  
  protected final MutableNumericSummaryType value;
  
  public BaseNumericSummaryFillPolicy(final NumericSummaryInterpolatorConfig config) {
    this.config = config;
    value = new MutableNumericSummaryType();
  }

  @Override
  public Collection<Integer> summariesAvailable() {
    return value.summariesAvailable();
  }

  @Override
  public NumericType value(int summary) {
    return value.value(summary);
  }

  @Override
  public NumericSummaryType fill() {
    // re-do fill
    value.clear();
    for (final int summary : config.expectedSummaries()) {
      FillPolicy fill = config.fillPolicy(summary);
      if (fill == null) {
        fill = config.defaultFillPolicy();
      }
      switch(fill) {
      case NONE:
      case NULL:
        // nothing to do
        break;
      case ZERO:
        value.set(summary, 0L);
        break;
      case NOT_A_NUMBER:
        value.set(summary, Double.NaN);
        break;
      case MIN:
        value.set(summary, Double.MIN_VALUE);
        break;
      case MAX:
        value.set(summary, Double.MAX_VALUE);
        break;
      }
    }
    return this;
  }

  @Override
  public FillWithRealPolicy realPolicy() {
    return config.defaultRealFillPolicy();
  }
  
  public FillWithRealPolicy realPolicy(final int summary) {
    return config.realFillPolicy(summary);
  }

  @Override
  public QueryInterpolatorConfig config() {
    return config;
  }
}

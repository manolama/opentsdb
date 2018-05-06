package net.opentsdb.data.types.numeric;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;

public class MutableNumericSummaryType implements NumericSummaryType {

  private Map<Integer, NumericType> values;
  
  public MutableNumericSummaryType() {
    values = Maps.newHashMapWithExpectedSize(1);
  }
  
  @Override
  public Collection<Integer> summariesAvailable() {
    return values.keySet();
  }

  @Override
  public NumericType value(final int summary) {
    return values.get(summary);
  }

  public void clear() {
    values.clear();
  }
  
  public void set(final int summary, final long value) {
    final MutableNumericType clone = new MutableNumericType();
    clone.set(value);
    values.put(summary, clone);
  }
  
  public void set(final int summary, final double value) {
    final MutableNumericType clone = new MutableNumericType();
    clone.set(value);
    values.put(summary, clone);
  }
}

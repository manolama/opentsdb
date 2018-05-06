package net.opentsdb.data.types.numeric;

import java.util.Collection;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

public interface NumericSummaryType extends TimeSeriesDataType {
  public static final TypeToken<NumericSummaryType> TYPE = 
      TypeToken.of(NumericSummaryType.class);
  
  public Collection<Integer> summariesAvailable();
  
  public NumericType value(final int summary);
  
  
}

package net.opentsdb.data.types.alert;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericType;

public interface AlertType extends TimeSeriesDataType<AlertType>{
  
  public static final TypeToken<AlertType> TYPE = TypeToken.of(AlertType.class);
  
  public static final Set<TypeToken<? extends TimeSeriesDataType>> TYPES = 
      Sets.newHashSet(TYPE);
  
  public String message();
  
  public NumericType dataPoint();
  
}

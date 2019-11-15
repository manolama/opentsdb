package net.opentsdb.data.types.alert;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;

public class AlertTypeList {

  private final List<AlertValue> values;
  
  public AlertTypeList() {
    values = Lists.newArrayList();
  }
  
  public void add(final AlertValue value) {
    values.add(value);
  }
  
  public TypedTimeSeriesIterator<AlertType> getIterator() {
    return new AlertTypeIterator();
  }
  
  class AlertTypeIterator implements TypedTimeSeriesIterator<AlertType> {
    private int idx = 0;

    @Override
    public boolean hasNext() {
      return idx < values.size();
    }

    @Override
    public TimeSeriesValue<AlertType> next() {
      return values.get(idx++);
    }

    @Override
    public TypeToken<AlertType> getType() {
      return AlertType.TYPE;
    }
    
  }
}

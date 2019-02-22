package net.opentsdb.query.interpolation.types.numeric;

import org.openjdk.jol.info.ClassLayout;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.pools.Allocator;

public class NumericInterpolatorAllocator extends BaseTSDBPlugin implements Allocator {
  private static final int SIZE = 
      (int) (ClassLayout.parseClass(PartialNumericInterpolator.class).instanceSize()
            + (ClassLayout.parseClass(MutableNumericValue.class).instanceSize() * 2));
  
  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public Object allocate() {
    return new PartialNumericInterpolator();
  }

  @Override
  public void deallocate(Object object) {
    try {
      ((PartialNumericInterpolator) object).close();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public TypeToken<?> dataType() {
    return TypeToken.of(PartialNumericInterpolator.class);
  }

  @Override
  public String type() {
    return "NumericInterpolatorAllocator";
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }

}

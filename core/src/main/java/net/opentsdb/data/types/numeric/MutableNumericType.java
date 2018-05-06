package net.opentsdb.data.types.numeric;

public class MutableNumericType implements NumericType {
  /** True if the value is stored as a long. */
  private boolean is_integer = true;
  
  /** A long value or a double encoded on a long if {@code is_integer} is false. */
  private long value = 0;
  
  public void set(final long value) {
    this.value = value;
    is_integer = true;
  }
  
  public void set(final double value) {
    this.value = Double.doubleToLongBits(value);
    is_integer = false;
  }
  
  public void set(final NumericType value) {
    if (value.isInteger()) {
      this.value = value.longValue();
      is_integer = true;
    } else {
      this.value = Double.doubleToRawLongBits(value.doubleValue());
      is_integer = false;
    }
  }
  
  @Override
  public boolean isInteger() {
    return is_integer;
  }

  @Override
  public long longValue() {
    if (is_integer) {
      return value;
    }
    throw new ClassCastException("Not a long in " + toString());
  }

  @Override
  public double doubleValue() {
    if (!is_integer) {
      return Double.longBitsToDouble(value);
    }
    throw new ClassCastException("Not a double in " + toString());
  }

  @Override
  public double toDouble() {
    if (is_integer) {
      return value;
    }
    return Double.longBitsToDouble(value);
  }
  
  @Override
  public String toString() {
    return is_integer ? Long.toString(value) : Double.toString(Double.longBitsToDouble(value));
  }
}

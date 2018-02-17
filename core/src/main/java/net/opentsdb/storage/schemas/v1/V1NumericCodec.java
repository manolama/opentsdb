package net.opentsdb.storage.schemas.v1;

import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.Const;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.storage.StorageSeries;

public class V1NumericCodec implements V1Codec {

  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The qualifier to parse
   * @param offset An offset from the start of the byte array
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte[] qualifier, 
      final int offset) {
    return inMilliseconds(qualifier[offset]);
  }
  
  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The qualifier to parse
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte[] qualifier) {
    return inMilliseconds(qualifier[0]);
  }
  
  /**
   * Determines if the qualifier is in milliseconds or not
   * @param qualifier The first byte of a qualifier
   * @return True if the qualifier is in milliseconds, false if not
   * @since 2.0
   */
  public static boolean inMilliseconds(final byte qualifier) {
    return (qualifier & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG;
  }
  
  @Override
  public TypeToken<?> type() {
    return NumericType.TYPE;
  }

  @Override
  public StorageSeries newIterable() {
    return new V1NumericStorageSeries();
  }
  
  

}

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
package net.opentsdb.data.types.numeric;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;

/**
 * For summaries we'll do:
 * <8B timestamp><1B num following values><1B type><1B flags><nB value>...[repeat]<8B terminal>
 * 
 * where the terminator is a 1 byte type with a value of 0. We'll shift types by one in storage 
 * so if "sum == 0" in our config, then in the type field it will be "1". That way we
 * have a useful terminator. An empty set would just have a type of 0 without data.
 * 
 * @since 3.0
 */
public interface NumericByteArraySummaryType extends TimeSeriesDataType {
  public static final TypeToken<NumericByteArraySummaryType> TYPE = 
      TypeToken.of(NumericByteArraySummaryType.class);
  
  /** Flag set when the timestamp is in milliseconds instead of seconds or 
   * nanoseconds. */
  public static final long MILLISECOND_FLAG = 0x4000000000000000L;
  
  /** Flag set when the timestamp is in nanoseconds in which case the next 
   * long is the nanos offset. */
  public static final long NANOSECOND_FLAG = 0x2000000000000000L;
  
  /** Flag set when the long value is an encoded double. Otherwise it's a 
   * straight long. */
  public static final byte FLOAT_FLAG = (byte) 0x80;
  
  /** Indicates this is the last long in the set. */
  public static final long TERIMNAL_FLAG = 0x1000000000000000L;
  
  /** A mask to zero out the flag bits before reading the timestamp. */
  public static final long TIMESTAMP_MASK = 0xFFFFFFFFFFFFFFFL;
  
  @Override
  default TypeToken<? extends TimeSeriesDataType> type() {
    return TYPE;
  }
  
  public int offset();
  
  public int end();
  
  public byte[] data();
  
}
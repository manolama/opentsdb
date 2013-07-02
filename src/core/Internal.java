// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

/**
 * <strong>This class is not part of the public API.</strong>
 * <p><pre>
 * ,____________________________,
 * | This class is reserved for |
 * | OpenTSDB's internal usage! |
 * `----------------------------'
 *       \                   / \  //\
 *        \    |\___/|      /   \//  \\
 *             /0  0  \__  /    //  | \ \
 *            /     /  \/_/    //   |  \  \
 *            @_^_@'/   \/_   //    |   \   \
 *            //_^_/     \/_ //     |    \    \
 *         ( //) |        \///      |     \     \
 *       ( / /) _|_ /   )  //       |      \     _\
 *     ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
 *   (( / / )) ,-{        _      `-.|.-~-.           .~         `.
 *  (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
 *  (( /// ))      `.   {            }                   /      \  \
 *   (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
 *              ///.----../        \             _ -~             `.  ^-`  ^-_
 *                ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
 *                                                                   /.-~
 *              You've been warned by the dragon!
 * </pre><p>
 * This class is reserved for OpenTSDB's own internal usage only.  If you use
 * anything from this package outside of OpenTSDB, a dragon will spontaneously
 * appear and eat you.  You've been warned.
 * <p>
 * This class only exists because Java's packaging system is annoying as the
 * "package-private" accessibility level only applies to the current package
 * but not its sub-packages,  and because Java doesn't have fine-grained API
 * visibility mechanism such as that of Scala or C++.
 * <p>
 * This package provides access into internal methods for higher-level
 * packages, for the sake of reducing code duplication and (ab)use of
 * reflection.
 */
public final class Internal {

  /** @see Const#FLAG_BITS  */
  public static final short FLAG_BITS = Const.FLAG_BITS;

  /** @see Const#LENGTH_MASK  */
  public static final short LENGTH_MASK = Const.LENGTH_MASK;

  /** @see Const#FLAGS_MASK  */
  public static final short FLAGS_MASK = Const.FLAGS_MASK;

  private Internal() {
    // Can't instantiate.
  }

  /** @see TsdbQuery#getScanner */
  public static Scanner getScanner(final Query query) {
    return ((TsdbQuery) query).getScanner();
  }

  /** @see RowKey#metricName */
  public static String metricName(final TSDB tsdb, final byte[] id) {
    return RowKey.metricName(tsdb, id);
  }

  /** Extracts the timestamp from a row key.  */
  public static long baseTime(final TSDB tsdb, final byte[] row) {
    return Bytes.getUnsignedInt(row, tsdb.metrics.width());
  }

  /** @see Tags#getTags */
  public static Map<String, String> getTags(final TSDB tsdb, final byte[] row) {
    return Tags.getTags(tsdb, row);
  }

  /** @see RowSeq#extractIntegerValue */
  public static long extractIntegerValue(final byte[] values,
                                         final int value_idx,
                                         final byte flags) {
    return RowSeq.extractIntegerValue(values, value_idx, flags);
  }

  /** @see RowSeq#extractFloatingPointValue */
  public static double extractFloatingPointValue(final byte[] values,
                                                 final int value_idx,
                                                 final byte flags) {
    return RowSeq.extractFloatingPointValue(values, value_idx, flags);
  }

  /** @see TSDB#metrics_width() */
  public static short metricWidth(final TSDB tsdb) {
    return tsdb.metrics.width();
  }

  /** @see CompactionQueue#complexCompact  */
  public static KeyValue complexCompact(final KeyValue kv) {
    final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(kv);
    return CompactionQueue.complexCompact(kvs, kv.qualifier().length / 2, false);
  }

  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier
   * @param qualifier The qualifier to parse
   * @return The offset in milliseconds from the base time
   * @throws IllegalArgument if the qualifier is null or empty
   * @since 2.0
   */
  public static int getOffsetFromQualifier(final byte[] qualifier) {
    return getOffsetFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier at the given offset (for compacted columns)
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The offset in milliseconds from the base time
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static int getOffsetFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    if ((qualifier[offset + 0] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return (int)(Bytes.getUnsignedInt(qualifier, offset) & 0x0FFFFFC0) 
        >>> (Const.FLAG_BITS + 2);        
    } else {
      final int seconds = (Bytes.getUnsignedShort(qualifier, offset) & 0xFFFF) 
        >>> Const.FLAG_BITS;
      return seconds * 1000;
    }
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgument if the qualifier is null or empty
   * @since 2.0
   */
  public static short getValueLengthFromQualifier(final byte[] qualifier) {
    return getValueLengthFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getValueLengthFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    short length;
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      length = (short) (qualifier[offset + 3] & Internal.LENGTH_MASK); 
    } else {
      length = (short) (qualifier[offset + 1] & Internal.LENGTH_MASK);
    }
    return (short) (length + 1);
  }

  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @return The length of the qualifier in bytes
   * @throws IllegalArgument if the qualifier is null or empty
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier) {
    return getQualifierLength(qualifier, 0);
  }
  
  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the qualifier in bytes
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    if ((qualifier[offset] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      if ((offset + 4) > qualifier.length) {
        throw new IllegalArgumentException(
            "Detected a millisecond flag but qualifier length is too short");
      }
      return 4;
    } else {
      if ((offset + 2) > qualifier.length) {
        throw new IllegalArgumentException("Qualifier length is too short");
      }
      return 2;
    }
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgument if the qualifier is null or empty
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier);
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @param offset An offset within the byte array
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time, final int offset) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier, offset);
  }

  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgument if the qualifier is null or empty
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier) {
    return getFlagsFromQualifier(qualifier, 0);
  }
  
  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    if ((qualifier[offset + 0] & Const.MS_BYTE_FLAG) == Const.MS_BYTE_FLAG) {
      return (short) (qualifier[offset + 3] & Internal.FLAGS_MASK); 
    } else {
      return (short) (qualifier[offset + 1] & Internal.FLAGS_MASK);
    }
  }

  /**
   * Returns a 2 or 4 byte qualifier based on the timestamp and the flags. If
   * the timestamp is in seconds, this returns a 2 byte qualifier. If it's in
   * milliseconds, returns a 4 byte qualifier 
   * @param timestamp A Unix epoch timestamp in seconds or milliseconds
   * @param flags Flags to set on the qualifier (length &| float)
   * @return A 2 or 4 byte qualifier for storage in column or compacted column
   * @since 2.0
   */
  public static byte[] buildQualifier(final long timestamp, final short flags) {
    final long base_time;
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) - ((timestamp / 1000) 
          % Const.MAX_TIMESPAN));
      final int qual = (int) (((timestamp - (base_time * 1000) 
          << (Const.FLAG_BITS + 2)) | flags) | Const.MS_FLAG);
      return Bytes.fromInt(qual);
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
      final short qual = (short) ((timestamp - base_time) << Const.FLAG_BITS
          | flags);
      return Bytes.fromShort(qual);
    }
  }

  /**
   * Checks the qualifier to verify that it has data and that the offset is
   * within bounds
   * @param qualifier The qualifier to validate
   * @param offset An optional offset
   * @throws IllegalArgument if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  private static void validateQualifier(final byte[] qualifier, 
      final int offset) {
    if (qualifier == null || qualifier.length < 0) {
      throw new IllegalArgumentException("Null or empty qualifier");
    }
    if (offset < 0 || offset >= qualifier.length - 1) {
      throw new IllegalArgumentException("Offset of [" + offset + 
          "] is greater than the qualifier length [" + qualifier.length + "]");
    }
  }
}

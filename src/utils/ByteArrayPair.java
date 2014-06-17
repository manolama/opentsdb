// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import org.hbase.async.Bytes;

/**
 * Simple helper class to store a pair of byte arrays for use in situations
 * where a map or Map.Entry doesn't make sense. Either key or value in this
 * class can be null but NOT both.
 * Sorting is performed on the key first, then on the value.
 */
public class ByteArrayPair implements Comparable<ByteArrayPair> {

  /** The key of the pair, may be null */
  final private byte[] key;
  
  /** The value of the pair, may be null */
  final private byte[] value;
  
  /**
   * Default constructor initializes the object
   * @param key The key to store, may be null
   * @param value The value to store, may be null
   * @throws IllegalArgumentException If both values are null
   */
  public ByteArrayPair(final byte[] key, final byte[] value) {
    if (key == null && value == null) {
      throw new IllegalArgumentException("Key and value cannot be null");
    }
    this.key = key;
    this.value = value;
  }
  
  /**
   * Sorts on the key first then on the value. Nulls are allowed and are ordered
   * first.
   * @param a The value to compare against.
   */
  public int compareTo(ByteArrayPair a) {
    final int key_compare = Bytes.memcmpMaybeNull(this.key, a.key);
    if (key_compare == 0) {
      return Bytes.memcmpMaybeNull(this.value, a.value);
    }
    return key_compare;
  }
  
  /** @return The key byte array */
  public final byte[] getKey() {
    return key;
  }
  
  /** @return The value byte array */
  public final byte[] getValue() {
    return value;
  }
  
}

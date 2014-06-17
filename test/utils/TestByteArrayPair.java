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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestByteArrayPair {
  final byte[] key = new byte[] { 1 };
  final byte[] val = new byte[] { 2 };
  final byte[] val2 = new byte[] { 3 };
  
  @Test
  public void setBoth() {
    final ByteArrayPair pair = new ByteArrayPair(key, val);
    assertNotNull(pair);
    assertArrayEquals(key, pair.getKey());
    assertArrayEquals(val, pair.getValue());
  }
  
  @Test
  public void nullKey() {
    final ByteArrayPair pair = new ByteArrayPair(null, val);
    assertNotNull(pair);
    assertNull(pair.getKey());
    assertArrayEquals(val, pair.getValue());
  }
  
  @Test
  public void nullValue() {
    final ByteArrayPair pair = new ByteArrayPair(key, null);
    assertNotNull(pair);
    assertArrayEquals(key, pair.getKey());
    assertNull(pair.getValue());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void bothNull() {
    new ByteArrayPair(null, null);
  }
  
  @Test
  public void sortTest() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(2);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(key, val));
    Collections.sort(pairs);
    assertArrayEquals(key, pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(val, pairs.get(1).getKey());
    assertArrayEquals(key, pairs.get(1).getValue());
  }
  
  @Test
  public void sortTestWithNullKey() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(2);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(null, val));
    Collections.sort(pairs);
    assertNull(pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(val, pairs.get(1).getKey());
    assertArrayEquals(key, pairs.get(1).getValue());
  }
  
  @Test
  public void sortTestonValue() {
    List<ByteArrayPair> pairs = new ArrayList<ByteArrayPair>(3);
    pairs.add(new ByteArrayPair(val, key));
    pairs.add(new ByteArrayPair(key, val2));
    pairs.add(new ByteArrayPair(key, val));
    
    Collections.sort(pairs);
    assertArrayEquals(key, pairs.get(0).getKey());
    assertArrayEquals(val, pairs.get(0).getValue());
    assertArrayEquals(key, pairs.get(1).getKey());
    assertArrayEquals(val2, pairs.get(1).getValue());
    assertArrayEquals(val, pairs.get(2).getKey());
    assertArrayEquals(key, pairs.get(2).getValue());
  }
}

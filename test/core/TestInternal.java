// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import net.opentsdb.utils.DateTime;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public final class TestInternal {

  @Test
  public void getValueLengthFromQualifierInt8() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 7 }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt8also() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x0F }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt1() {
    assertEquals(1, Internal.getValueLengthFromQualifier(new byte[] { 0, 0 }));
  }
  
  @Test
  public void getValueLengthFromQualifierInt4() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat4() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 11 }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat4also() {
    assertEquals(4, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x1B }));
  }
  
  @Test
  public void getValueLengthFromQualifierFloat8() {
    assertEquals(8, Internal.getValueLengthFromQualifier(new byte[] { 0, 0x1F }));
  }
  
  // since all the qualifier methods share the validateQualifier() method, we
  // can test them once
  @Test (expected = IllegalArgumentException.class)
  public void getValueLengthFromQualifierNull() {
    Internal.getValueLengthFromQualifier(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getValueLengthFromQualifierEmpty() {
    Internal.getValueLengthFromQualifier(new byte[0]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getValueLengthFromQualifierNegativeOffset() {
    Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, -42);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getValueLengthFromQualifierBadOffset() {
    Internal.getValueLengthFromQualifier(new byte[] { 0, 0x4B }, 42);
  }

  @Test
  public void getQualifierLengthSeconds() {
    assertEquals(2, Internal.getQualifierLength(new byte[] { 0, 0x0F }));
  }
  
  @Test
  public void getQualifierLengthMilliSeconds() {
    assertEquals(4, Internal.getQualifierLength(
        new byte[] { (byte) 0xF0, 0x00, 0x00, 0x07 }));
  }

  @Test (expected = IllegalArgumentException.class)
  public void getQualifierLengthSecondsTooShort() {
    Internal.getQualifierLength(new byte[] { 0x0F });
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getQualifierLengthMilliSecondsTooShort() {
    Internal.getQualifierLength(new byte[] { (byte) 0xF0, 0x00, 0x00,  });
  }

  @Test
  public void getTimestampFromQualifier() {
    final long ts = Internal.getTimestampFromQualifier(
        new byte[] { 0x00, 0x37 }, 1356998400);
    assertEquals(1356998403000L, ts);
  }
  
  @Test
  public void getTimestampFromQualifierMs() {
    final long ts = Internal.getTimestampFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 1356998400);
    assertEquals(1356998400008L, ts);
  }

  @Test
  public void getOffsetFromQualifier() {
    assertEquals(3000, Internal.getOffsetFromQualifier(
        new byte[] { 0x00, 0x37 }));
  }
  
  @Test
  public void getOffsetFromQualifierMs() {
    assertEquals(8, Internal.getOffsetFromQualifier(
        new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }));
  }

  @Test
  public void getOffsetFromQualifierOffset() {
    final byte[] qual = { 0x00, 0x37, 0x00, 0x47 };
    assertEquals(4000, Internal.getOffsetFromQualifier(qual, 2));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getOffsetFromQualifierBadOffset() {
    final byte[] qual = { 0x00, 0x37, 0x00, 0x47 };
    assertEquals(4000, Internal.getOffsetFromQualifier(qual, 3));
  }
  
  @Test
  public void getOffsetFromQualifierOffsetMixed() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertEquals(8, Internal.getOffsetFromQualifier(qual, 2));
  }

  @Test
  public void getFlagsFromQualifierInt() {
    assertEquals(7, Internal.getFlagsFromQualifier(new byte[] { 0x00, 0x37 }));
  }
  
  @Test
  public void getFlagsFromQualifierFloat() {
    assertEquals(11, Internal.getFlagsFromQualifier(new byte[] { 0x00, 0x1B }));
  }
  
  @Test
  public void buildQualifierSecond8ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 7);
    assertArrayEquals(new byte[] { 0x00, 0x37 }, q);
  }

  @Test
  public void buildQualifierSecond6ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 5);
    assertArrayEquals(new byte[] { 0x00, 0x35 }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 3);
    assertArrayEquals(new byte[] { 0x00, 0x33 }, q);
  }
  
  @Test
  public void buildQualifierSecond2ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 1);
    assertArrayEquals(new byte[] { 0x00, 0x31 }, q);
  }
  
  @Test
  public void buildQualifierSecond1ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998403, (short) 0);
    assertArrayEquals(new byte[] { 0x00, 0x30 }, q);
  }
  
  @Test
  public void buildQualifierSecond8ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998403, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { 0x00, 0x3F }, q);
  }
  
  @Test
  public void buildQualifierSecond4ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998403, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] { 0x00, 0x3B }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 7);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x07 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond6ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 5);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x05 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond4ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 3);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x03 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond2ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 1);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x01 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond1ByteLong() {
    final byte[] q = Internal.buildQualifier(1356998400008L, (short) 0);
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x00 }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond8ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998400008L, 
        (short) ( 7 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x0F }, q);
  }
  
  @Test
  public void buildQualifierMilliSecond4ByteFloat() {
    final byte[] q = Internal.buildQualifier(1356998400008L, 
        (short) ( 3 | Const.FLAG_FLOAT));
    assertArrayEquals(new byte[] {(byte) 0xF0, 0x00, 0x02, 0x0B }, q);
  }

  @Test
  public void extractQualifierSeconds() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertArrayEquals(new byte[] { 0, 0x47 }, 
        Internal.extractQualifier(qual, 6));
  }
  
  @Test
  public void extractQualifierMilliSeconds() {
    final byte[] qual = { 0x00, 0x37, (byte) 0xF0, 0x00, 0x02, 0x07, 0x00, 
        0x47 };
    assertArrayEquals(new byte[] { (byte) 0xF0, 0x00, 0x02, 0x07 }, 
        Internal.extractQualifier(qual, 2));
  }
}

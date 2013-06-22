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
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
               "ch.qos.*", "org.slf4j.*",
               "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
  Config.class, RowKey.class })
public final class TestRowSeq {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(config, "enable_milliseconds", true);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(config.enable_milliseconds()).thenReturn(true);
    when(tsdb.metrics.width()).thenReturn((short)3);
    when(RowKey.metricName(tsdb, KEY)).thenReturn("sys.cpu.user");
  }
  
  @Test
  public void setRow() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    assertEquals(2, rs.size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void setRowAlreadySet() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    assertEquals(2, rs.size());
    rs.setRow(kv);
  }
  
  @Test
  public void addRowReplace() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(2, rs.size());
    assertEquals(1356998403000L, rs.timestamp(0));
    assertEquals(1356998404000L, rs.timestamp(1));
  }
  
  @Test
  public void addRow() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    final byte[] row2 = { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
    rs.addRow(new KeyValue(row2, FAMILY, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(1357002003000L, rs.timestamp(2));
    assertEquals(1357002004000L, rs.timestamp(3));
  }
  
  @Test (expected = IllegalStateException.class)
  public void addRowNotSet() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.addRow(kv);
  }
  
  @Test
  public void timestamp() throws Exception {
    when(config.enable_milliseconds()).thenReturn(false);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
  }
  
  @Test
  public void timestampNormalizeMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMixedNormalized() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMixedNonNormalized() throws Exception {
    when(config.enable_milliseconds()).thenReturn(false);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void timestampOutofBounds() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
    rs.timestamp(2);
  }
  
  @Test
  public void iterateNormalizedMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);

    assertEquals(2, rs.size());
    
    final SeekableView it = rs.iterator();
    DataPoint dp = it.next();
    
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
 
    dp = it.next();    
    assertEquals(1356998402000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterateMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);

    final SeekableView it = rs.iterator();
    DataPoint dp = it.next();
    
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
 
    dp = it.next();    
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertFalse(it.hasNext());
  }
  
  /** Shorthand to create a {@link KeyValue}.  */
  private static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    return new KeyValue(KEY, FAMILY, qualifier, value);
  }
  
}

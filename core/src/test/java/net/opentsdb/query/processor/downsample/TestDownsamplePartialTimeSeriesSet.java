package net.opentsdb.query.processor.downsample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


import org.junit.Before;
import org.junit.Test;

import net.opentsdb.data.NoDataPartialTimeSeries;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.SecondTimeStamp;

public class TestDownsamplePartialTimeSeriesSet {

  private Downsample node;
  
  @Before
  public void before() throws Exception {
    node = mock(Downsample.class);
    
  }
  
  @Test
  public void handleMultipleAlignedInOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // final, d
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
  }
  
  @Test
  public void handleMultipleAlignedInOrderLostRace() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // ******* OOPS ******
    // now pass in c AGAIN
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(2, set.set_boundaries.get(5)); // now we have two!
    
    // final, d
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(2, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
  }
  
  @Test
  public void handleMultipleAlignedInOrderNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // final, d
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d is at 2 now
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in c, shifts d over.
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // final, a
    when(pts.set()).thenReturn(set_a);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
  }

  @Test
  public void handleMultipleAlignedOutOfOrderNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // d is at 2 now
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // now pass in c, shifts d over.
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // final, a
    when(pts.set()).thenReturn(set_a);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(7));
  }
  
  @Test
  public void handleMultipleAlignedOutOfOrderComplete() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_d = mock(PartialTimeSeriesSet.class);
    
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559433600));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_a.complete()).thenReturn(true);
    when(set_a.timeSeriesCount()).thenReturn(1);
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559455200));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_b.complete()).thenReturn(true);
    when(set_b.timeSeriesCount()).thenReturn(1);
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559476800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_c.complete()).thenReturn(true);
    when(set_c.timeSeriesCount()).thenReturn(1);
    
    when(set_d.start()).thenReturn(new SecondTimeStamp(1559498400));
    when(set_d.end()).thenReturn(new SecondTimeStamp(1559520000));
    when(set_d.complete()).thenReturn(true);
    when(set_d.timeSeriesCount()).thenReturn(1);
    
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559433600  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d is at 2 now
    when(pts.set()).thenReturn(set_d);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // d
    temp = set.set_boundaries.get(2);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in c, shifts d over.
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // d
    temp = set.set_boundaries.get(4);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // final, a
    when(pts.set()).thenReturn(set_a);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(1, set.count.get());
    
    // now it's a
    temp = set.set_boundaries.get(0);
    assertEquals(1559433600, temp >>> 32);
    assertEquals(1559455200, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559455200, temp >>> 32);
    assertEquals(1559476800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559476800, temp >>> 32);
    assertEquals(1559498400, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
    
    // d
    temp = set.set_boundaries.get(6);
    assertEquals(1559498400, temp >>> 32);
    assertEquals(1559520000, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(7));
  }

  @Test
  public void handleMultipleNotAlignedInOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
  }
  
  @Test
  public void handleMultipleNotAlignedInOrderNoData() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    NoDataPartialTimeSeries pts = mock(NoDataPartialTimeSeries.class);
    when(pts.set()).thenReturn(set_a);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // a is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // now pass in b
    when(pts.set()).thenReturn(set_b);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertTrue(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(0, set.set_boundaries.get(5));
  }
  
  @Test
  public void handleMultipleNotAlignedOutOfOrder() throws Exception {
    PartialTimeSeriesSet set_a = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_b = mock(PartialTimeSeriesSet.class);
    PartialTimeSeriesSet set_c = mock(PartialTimeSeriesSet.class);
    
    // funky 10 hour intervals
    when(set_a.start()).thenReturn(new SecondTimeStamp(1559512800));
    when(set_a.end()).thenReturn(new SecondTimeStamp(1559548800));
    
    when(set_b.start()).thenReturn(new SecondTimeStamp(1559548800));
    when(set_b.end()).thenReturn(new SecondTimeStamp(1559584800));
    
    when(set_c.start()).thenReturn(new SecondTimeStamp(1559584800));
    when(set_c.end()).thenReturn(new SecondTimeStamp(1559620800));
    
    
    // same settings
    long[] sizes = new long[] {
        21600_000,  // 6h
        86400_000,  // 1d
        1,          // num new sets
        1559520000  // start at midnight
    };
    when(node.getSizes("m1")).thenReturn(sizes);
    
    DownsamplePartialTimeSeriesSet set = new DownsamplePartialTimeSeriesSet();
    set.reset(node, "m1", 0);
    
    PartialTimeSeries pts = mock(PartialTimeSeries.class);
    when(pts.set()).thenReturn(set_b);
    
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // b is at 0
    long temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // now pass in c
    when(pts.set()).thenReturn(set_c);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertFalse(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // first entry stays the same, b
    temp = set.set_boundaries.get(0);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // c
    temp = set.set_boundaries.get(2);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // now pass in a
    when(pts.set()).thenReturn(set_a);
    set.handleMultiples(pts);
    
    assertEquals(8, set.set_boundaries.length());
    assertEquals(4, set.completed_array.length);
    assertTrue(set.all_sets_accounted_for.get());
    assertFalse(set.complete.get());
    assertEquals(0, set.count.get());
    
    // shift to a
    temp = set.set_boundaries.get(0);
    assertEquals(1559512800, temp >>> 32);
    assertEquals(1559548800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(1));
    
    // b
    temp = set.set_boundaries.get(2);
    assertEquals(1559548800, temp >>> 32);
    assertEquals(1559584800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(3));
    
    // c
    temp = set.set_boundaries.get(4);
    assertEquals(1559584800, temp >>> 32);
    assertEquals(1559620800, temp & DownsamplePartialTimeSeriesSet.END_MASK);
    assertEquals(1, set.set_boundaries.get(5));
  }
  
}

package net.opentsdb.query.interpolation.types.numeric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.MutableNumericSummaryValue;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolator;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericLERP;
import net.opentsdb.query.interpolation.types.numeric.ScalarNumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestNumericSummaryInterpolator {

  private NumericSummaryInterpolatorConfig config;
  private MockTimeSeries<NumericSummaryType> source;
  
  @Before
  public void before() throws Exception {
    config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .build();
    
    source = new MockTimeSeries<NumericSummaryType>(
        BaseTimeSeriesStringId.newBuilder()
          .setMetric("foo")
          .build(), 
        NumericSummaryType.TYPE);
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(0, 42);
    v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(0, 24);
    v.resetValue(2, 3);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    v.resetValue(2, 6);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(4000));
    v.resetValue(0, 1);
    v.resetValue(2, 1);
    source.add(v);
  }
  
  @Test
  public void ctorSource() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    ReadAheadNumericInterpolator rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(42, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(5, rani.next.longValue());
    assertEquals(1000, interpolator.next.timestamp().msEpoch());
    assertEquals(42, interpolator.next.value().value(0).longValue());
    assertEquals(5, interpolator.next.value().value(2).longValue());
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertTrue(interpolator.hasNext());
    
    try {
      new NumericSummaryInterpolator((TimeSeries) null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericSummaryInterpolator(source, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty summary
    source.data.clear();
    interpolator = new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertFalse(rani.hasNext());
    rani = interpolator.data.get(2);
    assertFalse(rani.hasNext());
    assertNull(interpolator.next);
    assertFalse(interpolator.hasNext());
    
    // test a ctor without a summary iterator
    MockTimeSeries<NumericType> non_summary = new MockTimeSeries<NumericType>(
        BaseTimeSeriesStringId.newBuilder()
          .setMetric("foo")
          .build(), 
        NumericType.TYPE);
    MutableNumericValue v = new MutableNumericValue();
    v.reset(new MillisecondTimeStamp(1000), 42);
    non_summary.add(v);
    
    interpolator = new NumericSummaryInterpolator(non_summary, config);
    assertSame(config, interpolator.config);
    assertNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertFalse(rani.hasNext());
    rani = interpolator.data.get(2);
    assertFalse(rani.hasNext());
    assertNull(interpolator.next);
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertFalse(interpolator.hasNext());
  }
  
  @Test
  public void ctorIterator() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source.iterator(NumericSummaryType.TYPE).get(), config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    ReadAheadNumericInterpolator rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(42, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(5, rani.next.longValue());
    assertEquals(1000, interpolator.next.timestamp().msEpoch());
    assertEquals(42, interpolator.next.value().value(0).longValue());
    assertEquals(5, interpolator.next.value().value(2).longValue());
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertTrue(interpolator.hasNext());
    
    try {
      new NumericSummaryInterpolator((Iterator) null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      new NumericSummaryInterpolator(source, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void ctorSync() throws Exception {
    config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setSync(true)
        .build();
    
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    ReadAheadNumericInterpolator rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(42, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(5, rani.next.longValue());
    assertEquals(1000, interpolator.next.timestamp().msEpoch());
    assertEquals(42, interpolator.next.value().value(0).longValue());
    assertEquals(5, interpolator.next.value().value(2).longValue());
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertTrue(interpolator.hasNext());
    
    source.data.clear();
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    //v.resetValue(0, 42);
    v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(0, 24);
    v.resetValue(2, 3);
    source.add(v);
    
    // staggered start
    interpolator = new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(2000, rani.nextReal().msEpoch());
    assertEquals(24, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(2000, rani.nextReal().msEpoch());
    assertEquals(3, rani.next.longValue());
    assertEquals(2000, interpolator.next.timestamp().msEpoch());
    assertEquals(24, interpolator.next.value().value(0).longValue());
    assertEquals(3, interpolator.next.value().value(2).longValue());
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertTrue(interpolator.hasNext());
    
    source.data.clear();
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    //v.resetValue(0, 42);
    v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(0, 24);
    //v.resetValue(2, 3);
    source.add(v);
    
    // Never any synced data
    interpolator = new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertFalse(rani.hasNext());
    rani = interpolator.data.get(2);
    assertFalse(rani.hasNext());
    assertNull(interpolator.next);
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertFalse(interpolator.hasNext());
    
    source.data.clear();
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    //v.resetValue(0, 42);
    v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(2000));
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    v.resetValue(2, 6);
    source.add(v);
    
    // push through a null
    interpolator = new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(3000, rani.nextReal().msEpoch());
    assertEquals(89, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(3000, rani.nextReal().msEpoch());
    assertEquals(6, rani.next.longValue());
    assertEquals(3000, interpolator.next.timestamp().msEpoch());
    assertEquals(89, interpolator.next.value().value(0).longValue());
    assertEquals(6, interpolator.next.value().value(2).longValue());
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertTrue(interpolator.hasNext());
    
    source.data.clear();
    
    // Empty source
    interpolator = new NumericSummaryInterpolator(source, config);
    assertSame(config, interpolator.config);
    assertNotNull(interpolator.iterator);
    assertEquals(2, interpolator.data.size());
    rani = interpolator.data.get(0);
    assertFalse(rani.hasNext());
    rani = interpolator.data.get(2);
    assertFalse(rani.hasNext());
    assertNull(interpolator.next);
    assertEquals(0, interpolator.response.timestamp().msEpoch());
    assertFalse(interpolator.hasNext());
  }
  
  @Test
  public void setReadAheads() throws Exception {
    source.data.clear();
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    // nothing in there yet
    ReadAheadNumericInterpolator rani = interpolator.data.get(0);
    assertFalse(rani.hasNext());
    rani = interpolator.data.get(2);
    assertFalse(rani.hasNext());
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(0, 42);
    v.resetValue(2, 5);
    
    // add a normal value
    interpolator.setReadAheads(v);
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(42, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(1000, rani.nextReal().msEpoch());
    assertEquals(5, rani.next.longValue());
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(0, 24);
    v.resetValue(2, 3);
    
    // add another normal value
    interpolator.setReadAheads(v);
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(2000, rani.nextReal().msEpoch());
    assertEquals(24, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(2000, rani.nextReal().msEpoch());
    assertEquals(3, rani.next.longValue());
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    //v.resetValue(2, 6);
    
    // add a partial
    interpolator.setReadAheads(v);
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(3000, rani.nextReal().msEpoch());
    assertEquals(89, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(2000, rani.nextReal().msEpoch());
    assertEquals(3, rani.next.longValue());
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(4000));
    v.resetValue(1, 1);
    v.resetValue(2, 1);
    
    // add one with an unexpected summary
    interpolator.setReadAheads(v);
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(3000, rani.nextReal().msEpoch());
    assertEquals(89, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(4000, rani.nextReal().msEpoch());
    assertEquals(1, rani.next.longValue());
    
    v = new MutableNumericSummaryValue();
    v.resetNull(new MillisecondTimeStamp(5000));
    
    // add a null value
    interpolator.setReadAheads(v);
    rani = interpolator.data.get(0);
    assertTrue(rani.hasNext());
    assertEquals(3000, rani.nextReal().msEpoch());
    assertEquals(89, rani.next.longValue());
    rani = interpolator.data.get(2);
    assertTrue(rani.hasNext());
    assertEquals(4000, rani.nextReal().msEpoch());
    assertEquals(1, rani.next.longValue());
    
    try {
      interpolator.setReadAheads(null);
      fail("Expected NullPointerException");
    } catch (NullPointerException e) { }
  }
  
  @Test
  public void nextReal() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    assertEquals(1000, interpolator.nextReal().msEpoch());
    
    interpolator.has_next = false;
    try {
      interpolator.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
    
    interpolator.has_next = true;
    interpolator.next = null;
    try {
      interpolator.nextReal();
      fail("Expected NoSuchElementException");
    } catch (NoSuchElementException e) { }
  }
  
  @Test
  public void fill() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    MutableNumericSummaryValue v = interpolator.fill(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    v = interpolator.fill(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(42, v.value().value(0).longValue());
    assertEquals(5, v.value().value(2).longValue());
    
    v = interpolator.fill(new MillisecondTimeStamp(1500));
    assertEquals(1500, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    // still nan as we haven't passed anything else to the read aheads
    v = interpolator.fill(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
  }
  
  @Test
  public void fillOutOfSync() throws Exception {
    source.data.clear();
    
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(0, 42);
    //v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    //v.resetValue(0, 24);
    v.resetValue(2, 3);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    //v.resetValue(2, 6);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(4000));
    v.resetValue(0, 1);
    v.resetValue(2, 1);
    source.add(v);
    
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    interpolator.setReadAheads(source.data.get(1));
    interpolator.setReadAheads(source.data.get(2));
    interpolator.setReadAheads(source.data.get(3));
    
    v = interpolator.fill(new MillisecondTimeStamp(500));
    assertEquals(500, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    v = interpolator.fill(new MillisecondTimeStamp(1000));
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(42, v.value().value(0).longValue());
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    v = interpolator.fill(new MillisecondTimeStamp(1500));
    assertEquals(1500, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    v = interpolator.fill(new MillisecondTimeStamp(2000));
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertTrue(Double.isNaN(v.value().value(0).doubleValue()));
    assertEquals(3, v.value().value(2).longValue());
    
    v = interpolator.fill(new MillisecondTimeStamp(3000));
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(89, v.value().value(0).longValue());
    assertTrue(Double.isNaN(v.value().value(2).doubleValue()));
    
    v = interpolator.fill(new MillisecondTimeStamp(4000));
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(2, v.value().summariesAvailable().size());
    assertEquals(1, v.value().value(0).longValue());
    assertEquals(1, v.value().value(2).longValue());
  }
  
  @Test
  public void nextAligned() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    long[] sums = new long[] { 42, 24, 89, 1 };
    long[] counts = new long[] { 5, 3, 6, 1 };
    long ts = 1000;
    int i = 0;
    while (interpolator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) interpolator.next(
              new MillisecondTimeStamp(ts));
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextAlignedSynced() throws Exception {
    config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setSync(true)
        .build();
    
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    long[] sums = new long[] { 42, 24, 89, 1 };
    long[] counts = new long[] { 5, 3, 6, 1 };
    long ts = 1000;
    int i = 0;
    while (interpolator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) interpolator.next(
              new MillisecondTimeStamp(ts));
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      assertEquals(sums[i], tsv.value().value(0).longValue());
      assertEquals(counts[i++], tsv.value().value(2).longValue());
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 1000L;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextInBetweenFills() throws Exception {
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    long[] sums = new long[] { -1, 42, -1, 24, -1, 89,  -1, 1 };
    long[] counts = new long[] { -1, 5, -1, 3, -1, 6, -1, 1 };
    long ts = 500;
    int i = 0;
    while (interpolator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) interpolator.next(
              new MillisecondTimeStamp(ts));
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 500;
      i++;
    }
    assertEquals(8, i);
  }
  
  @Test
  public void nextNotAligned() throws Exception {
    source.data.clear();
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(0, 42);
    //v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    //v.resetValue(0, 24);
    v.resetValue(2, 3);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    //v.resetValue(2, 6);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(4000));
    //v.resetValue(0, 1);
    v.resetValue(2, 1);
    source.add(v);
    
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    long[] sums = new long[] { 42, -1, 89, -1 };
    long[] counts = new long[] { -1, 3, -1, 1 };
    long ts = 1000;
    int i = 0;
    while (interpolator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) interpolator.next(
              new MillisecondTimeStamp(ts));
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 1000;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNotAlignedSynced() throws Exception {
    config = NumericSummaryInterpolatorConfig.newBuilder()
        .setDefaultFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setDefaultRealFillPolicy(FillWithRealPolicy.NONE)
        .addExpectedSummary(0)
        .addExpectedSummary(2)
        .setSync(true)
        .build();
    
    source.data.clear();
    MutableNumericSummaryValue v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(1000));
    v.resetValue(0, 42);
    //v.resetValue(2, 5);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(2000));
    v.resetValue(0, 24);
    v.resetValue(2, 3);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(3000));
    v.resetValue(0, 89);
    //v.resetValue(2, 6);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(4000));
    v.resetValue(0, 1);
    v.resetValue(2, 1);
    source.add(v);
    
    v = new MutableNumericSummaryValue();
    v.resetTimestamp(new MillisecondTimeStamp(5000));
    v.resetValue(0, 29);
    //v.resetValue(2, 2);
    source.add(v);
    
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
    
    long[] sums = new long[] { -1, 24, -1, 1 };
    long[] counts = new long[] { -1, 3, -1, 1 };
    long ts = 1000;
    int i = 0;
    while (interpolator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> tsv = 
          (TimeSeriesValue<NumericSummaryType>) interpolator.next(
              new MillisecondTimeStamp(ts));
      print(tsv);
      assertEquals(ts, tsv.timestamp().msEpoch());
      if (sums[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
      } else {
        assertEquals(sums[i], tsv.value().value(0).longValue());
      }
      if (counts[i] < 0) {
        assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
      } else {
        assertEquals(counts[i], tsv.value().value(2).longValue());
      }
      assertEquals(2, tsv.value().summariesAvailable().size());
      ts += 1000;
      i++;
    }
    assertEquals(4, i);
  }
  
  @Test
  public void nextNoData() throws Exception {
    source.data.clear();
    NumericSummaryInterpolator interpolator = 
        new NumericSummaryInterpolator(source, config);
   
    assertFalse(interpolator.hasNext());
    TimeSeriesValue<NumericSummaryType> tsv = 
        (TimeSeriesValue<NumericSummaryType>) interpolator.next(
            new MillisecondTimeStamp(1000));
    assertEquals(1000, tsv.timestamp().msEpoch());
    assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
    assertEquals(2, tsv.value().summariesAvailable().size());
    
    tsv = 
        (TimeSeriesValue<NumericSummaryType>) interpolator.next(
            new MillisecondTimeStamp(2000));
    assertEquals(2000, tsv.timestamp().msEpoch());
    assertTrue(Double.isNaN(tsv.value().value(0).doubleValue()));
    assertTrue(Double.isNaN(tsv.value().value(2).doubleValue()));
    assertEquals(2, tsv.value().summariesAvailable().size());
  }
  
  void print(final TimeSeriesValue<NumericSummaryType> tsv) {
    System.out.println("**** [UT] " + tsv.timestamp());
    if (tsv.value() == null) {
      System.out.println("**** [UT] Null value *****");
    } else {
      for (int summary : tsv.value().summariesAvailable()) {
        NumericType t = tsv.value().value(summary);
        if (t == null) {
          System.out.println("***** [UT] value for " + summary + " was null");
        } else {
          System.out.println("***** [UT] [" + summary + "] " + t.toDouble());
        }
      }
    }
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
  }
}

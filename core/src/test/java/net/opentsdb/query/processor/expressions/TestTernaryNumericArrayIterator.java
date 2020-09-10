package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;

public class TestTernaryNumericArrayIterator extends BaseNumericTest {

  private TimeSeries left;
  private TimeSeries right;
  private TimeSeries condition;
  
  @Test
  public void ctorNoCondition() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .build());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void ctorEmptyCondition() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertFalse(iterator.hasNext());
  }
  
  // Naming format is <condition><left><right>
  @Test
  public void longLongLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 4, 5, 2, 9 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void doubleLongLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0D);
    ((NumericArrayTimeSeries) condition).add(1D);
    ((NumericArrayTimeSeries) condition).add(1D);
    ((NumericArrayTimeSeries) condition).add(-1D);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 4, 5, 2, 9 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longDoubleLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1D);
    ((NumericArrayTimeSeries) left).add(5D);
    ((NumericArrayTimeSeries) left).add(2D);
    ((NumericArrayTimeSeries) left).add(8D);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, 5, 2, 9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longLongDouble() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4D);
    ((NumericArrayTimeSeries) right).add(10D);
    ((NumericArrayTimeSeries) right).add(8D);
    ((NumericArrayTimeSeries) right).add(9D);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, 5, 2, 9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void longDoubleDouble() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1D);
    ((NumericArrayTimeSeries) left).add(5D);
    ((NumericArrayTimeSeries) left).add(2D);
    ((NumericArrayTimeSeries) left).add(8D);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4D);
    ((NumericArrayTimeSeries) right).add(10D);
    ((NumericArrayTimeSeries) right).add(8D);
    ((NumericArrayTimeSeries) right).add(9D);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, 5, 2, 9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void doubleDoubleDouble() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1D);
    ((NumericArrayTimeSeries) left).add(5D);
    ((NumericArrayTimeSeries) left).add(2D);
    ((NumericArrayTimeSeries) left).add(8D);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4D);
    ((NumericArrayTimeSeries) right).add(10D);
    ((NumericArrayTimeSeries) right).add(8D);
    ((NumericArrayTimeSeries) right).add(9D);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0D);
    ((NumericArrayTimeSeries) condition).add(1D);
    ((NumericArrayTimeSeries) condition).add(1D);
    ((NumericArrayTimeSeries) condition).add(-1D);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, 5, 2, 9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void doubleNaNsLongLong() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0D);
    ((NumericArrayTimeSeries) condition).add(Double.NaN);
    ((NumericArrayTimeSeries) condition).add(1D);
    ((NumericArrayTimeSeries) condition).add(Double.NaN);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new long[] { 4, 10, 2, 9 },
        value.value().longArray());
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void longNullLong() throws Exception {
    right = new NumericArrayTimeSeries(RIGHT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) right).add(4);
    ((NumericArrayTimeSeries) right).add(10);
    ((NumericArrayTimeSeries) right).add(8);
    ((NumericArrayTimeSeries) right).add(9);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.RIGHT_KEY, right)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { 4, Double.NaN, Double.NaN, 9 },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void longLongNull() throws Exception {
    left = new NumericArrayTimeSeries(LEFT_ID, 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) left).add(1);
    ((NumericArrayTimeSeries) left).add(5);
    ((NumericArrayTimeSeries) left).add(2);
    ((NumericArrayTimeSeries) left).add(8);
    
    condition = new NumericArrayTimeSeries(mock(TimeSeriesId.class), 
        new SecondTimeStamp(60));
    ((NumericArrayTimeSeries) condition).add(0);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(1);
    ((NumericArrayTimeSeries) condition).add(-1);
    
    TernaryNumericArrayIterator iterator = 
        new TernaryNumericArrayIterator(node, RESULT, 
            (Map) ImmutableMap.builder()
              .put(ExpressionTimeSeries.LEFT_KEY, left)
              .put(ExpressionTimeSeries.CONDITION_KEY, condition)
              .build());
    assertTrue(iterator.hasNext());
    TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    assertArrayEquals(new double[] { Double.NaN, 5, 2, Double.NaN },
        value.value().doubleArray(), 0.001);
    assertEquals(60, value.timestamp().epoch());
    assertEquals(0, value.value().offset());
    assertEquals(4, value.value().end());
    assertFalse(iterator.hasNext());
  }
}


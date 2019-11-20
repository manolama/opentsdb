package net.opentsdb.query.egads;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.alert.AlertValue;

public class TestThresholdEvaluator {
  public static final TimeStamp BASE_TIME = new SecondTimeStamp(1546300800);
  
  @Test
  public void evalUpperOnlyPercent() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        50, false, 0, false, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 150, 100));
    AlertValue av = evaluator.eval(BASE_TIME, 151, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(151, av.dataPoint().doubleValue(), 0.001);
    assertEquals(150, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, -150, 100));
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
    
    evaluator = new ThresholdEvaluator(
        50, false, 0, false, 4, null, null, null, null);
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 150, 120.6));
    av = evaluator.eval(BASE_TIME, 151, 95.78);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(151, av.dataPoint().doubleValue(), 0.001);
    assertEquals(143.67, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, -150, 2.75));
    assertArrayEquals(new double[] { 150, 170.6, 145.78, 52.75 }, 
        evaluator.upperThresholds(), 0.001);
    assertArrayEquals(new double[] { 0, 0, 0, 0 }, evaluator.lowerThresholds(), 0.001);
  }
  
  @Test
  public void evalUpperOnlyScalar() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        100, true, 0, false, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 200, 100));
    AlertValue av = evaluator.eval(BASE_TIME, 200.1, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(200.1, av.dataPoint().doubleValue(), 0.001);
    assertEquals(200, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, -150, 100));
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
    
    evaluator = new ThresholdEvaluator(
        100, true, 0, false, 4, null, null, null, null);
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 220.6, 120.6));
    av = evaluator.eval(BASE_TIME, 221, 95.78);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(221, av.dataPoint().doubleValue(), 0.001);
    assertEquals(195.78, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, -150, 2.75));
    assertArrayEquals(new double[] { 200, 220.6, 195.78, 102.75 }, 
        evaluator.upperThresholds(), 0.001);
    assertArrayEquals(new double[] { 0, 0, 0, 0 }, evaluator.lowerThresholds(), 0.001);
  }
  
  @Test
  public void evalLowerOnlyPercent() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        0, false, 50, false, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 50, 100));
    AlertValue av = evaluator.eval(BASE_TIME, 49, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(49, av.dataPoint().doubleValue(), 0.001);
    assertEquals(50, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, 151, 100));
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
    
    evaluator = new ThresholdEvaluator(
        0, false, 50, false, 4, null, null, null, null);
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 70.6, 120.6));
    av = evaluator.eval(BASE_TIME, 45.77, 95.78);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(45.77, av.dataPoint().doubleValue(), 0.001);
    assertEquals(47.89, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, 150, 2.75));
    assertArrayEquals(new double[] { 0, 0, 0, 0 }, 
        evaluator.upperThresholds(), 0.001);
    assertArrayEquals(new double[] { 50, 70.6, 45.78, -47.25 }, evaluator.lowerThresholds(), 0.001);
  }
  
  @Test
  public void evalLowerOnlyScalar() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        0, false, 100, true, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 0, 100));
    AlertValue av = evaluator.eval(BASE_TIME, -0.5, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-0.5, av.dataPoint().doubleValue(), 0.001);
    assertEquals(0, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, 205, 100));
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
    
    evaluator = new ThresholdEvaluator(
        0, false, 100, true, 4, null, null, null, null);
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 20.6, 120.6));
    av = evaluator.eval(BASE_TIME, -5, 95.78);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-5, av.dataPoint().doubleValue(), 0.001);
    assertEquals(-4.219, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.eval(BASE_TIME, 201, 2.75));
    assertArrayEquals(new double[] { 0, 0, 0, 0 }, 
        evaluator.upperThresholds(), 0.001);
    assertArrayEquals(new double[] { 0, 20.6, -4.219, -97.25 }, evaluator.lowerThresholds(), 0.001);
  }

  @Test
  public void evalBoth() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        50, false, 50, false, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 150, 100));
    AlertValue av = evaluator.eval(BASE_TIME, 151, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(151, av.dataPoint().doubleValue(), 0.001);
    assertEquals(150, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    av = evaluator.eval(BASE_TIME, -150, 100);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-150, av.dataPoint().doubleValue(), 0.001);
    assertEquals(50, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
    
    evaluator = new ThresholdEvaluator(
        50, false, 50, false, 4, null, null, null, null);
    assertNull(evaluator.eval(BASE_TIME, 100, 100));
    assertNull(evaluator.eval(BASE_TIME, 150, 120.6));
    av = evaluator.eval(BASE_TIME, 151, 95.78);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(151, av.dataPoint().doubleValue(), 0.001);
    assertEquals(143.67, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    av = evaluator.eval(BASE_TIME, -150, 2.75);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-150, av.dataPoint().doubleValue(), 0.001);
    assertEquals(1.375, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertArrayEquals(new double[] { 150, 170.6, 145.78, 52.75 }, 
        evaluator.upperThresholds(), 0.001);
    assertArrayEquals(new double[] { 50, 70.6, 45.78, -47.25 }, evaluator.lowerThresholds(), 0.001);
  }

  @Test
  public void evalBothSmallValues() throws Exception {
    ThresholdEvaluator evaluator = new ThresholdEvaluator(
        50.5, false, 50.5, false, 0, null, null, null, null);
    
    assertNull(evaluator.eval(BASE_TIME, 0.02, 0.023));
    AlertValue av = evaluator.eval(BASE_TIME, 0.05, 0.023);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(0.05, av.dataPoint().doubleValue(), 0.001);
    assertEquals(0.034, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    
    av = evaluator.eval(BASE_TIME, 0.0023, 0.023);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(0.0023, av.dataPoint().doubleValue(), 0.001);
    assertEquals(0.0113, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    
    assertNull(evaluator.eval(BASE_TIME, -0.049, -0.050));
    av = evaluator.eval(BASE_TIME, -0.023, -0.050);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-0.023, av.dataPoint().doubleValue(), 0.001);
    assertEquals(-0.024, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.UPPER, av.thresholdType());
    
    av = evaluator.eval(BASE_TIME, -0.0756, -0.050);
    assertEquals(BASE_TIME.epoch(), av.timestamp().epoch());
    assertEquals(-0.075, av.dataPoint().doubleValue(), 0.001);
    assertEquals(-0.075, av.threshold().doubleValue(), 0.001);
    assertEquals(ThresholdEvaluator.LOWER, av.thresholdType());
    assertNull(evaluator.upperThresholds());
    assertNull(evaluator.lowerThresholds());
  }
  
  @Test
  public void foo() throws Exception {
    
  }
}

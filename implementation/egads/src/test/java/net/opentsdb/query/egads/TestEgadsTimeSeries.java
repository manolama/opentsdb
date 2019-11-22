package net.opentsdb.query.egads;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.alert.AlertValue;
import net.opentsdb.data.types.numeric.NumericArrayType;

public class TestEgadsTimeSeries {

  public static final TimeSeriesId ID = BaseTimeSeriesStringId.newBuilder()
      .setAlias("foo")
      .setNamespace("System")
      .setMetric("sys.cpu.user")
      .addTags("host", "web01")
      .addTags("dc", "den")
      .addAggregatedTag("service")
      .build();
  public static final TimeStamp BASE_TIME = new SecondTimeStamp(1546300800);
  public static final double[] RESULTS = new double[] { 42, 15, 24, 1.5 };
  
//  @Test
//  public void ctor() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, RESULTS, "prediction", 
//        "OlympicScoring", BASE_TIME);
//    assertEquals(ID.buildHashCode(), ts.originalHash());
//    assertNotEquals(ID.buildHashCode(), ts.id().buildHashCode());
//    TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
//    assertEquals("foo", id.alias());
//    assertEquals("System", id.namespace());
//    assertEquals("sys.cpu.user.prediction", id.metric());
//    assertEquals(3, id.tags().size());
//    assertEquals("web01", id.tags().get("host"));
//    assertEquals("den", id.tags().get("dc"));
//    assertEquals("OlympicScoring", id.tags().get("_anomalyModel"));
//    
//    // check types
//    assertEquals(1, ts.types().size());
//    assertTrue(ts.types().contains(NumericArrayType.TYPE));
//  }
//  
//  @Test
//  public void addAlerts() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, RESULTS, "prediction", 
//        "OlympicScoring", BASE_TIME);
//    assertNull(ts.alerts);
// 
//    // check types
//    assertEquals(1, ts.types().size());
//    assertTrue(ts.types().contains(NumericArrayType.TYPE));
//    
//    AlertValue a1 = mock(AlertValue.class);
//    AlertValue a2 = mock(AlertValue.class);
//    AlertValue a3 = mock(AlertValue.class);
//    
//    ts.addAlerts(Lists.newArrayList(a1, a2, a3));
//    assertEquals(3, ts.alerts.size());
//    assertEquals(2, ts.types().size());
//    assertTrue(ts.types().contains(NumericArrayType.TYPE));
//    assertTrue(ts.types().contains(AlertType.TYPE));
//    
//    // single add
//    ts = new EgadsTimeSeries(ID, RESULTS, "prediction", "OlympicScoring", BASE_TIME);
//    assertNull(ts.alerts);
//    assertEquals(1, ts.types().size());
//    assertTrue(ts.types().contains(NumericArrayType.TYPE));
//    
//    ts.addAlert(a1);
//    assertEquals(1, ts.alerts.size());
//    assertEquals(2, ts.types().size());
//    assertTrue(ts.types().contains(NumericArrayType.TYPE));
//    assertTrue(ts.types().contains(AlertType.TYPE));
//    
//    ts.addAlert(a2);
//    assertEquals(2, ts.alerts.size());
//  }
//  
//  @Test
//  public void iteratorNoAlerts() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, RESULTS, "prediction", 
//        "OlympicScoring", BASE_TIME);
//    TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
//        ts.iterator(NumericArrayType.TYPE).get();
//    assertTrue(it.hasNext());
//    
//    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) it.next();
//    assertEquals(BASE_TIME.epoch(), v.timestamp().epoch());
//    assertEquals(0, v.value().offset());
//    assertEquals(4, v.value().end());
//    assertFalse(v.value().isInteger());
//    assertArrayEquals(RESULTS, v.value().doubleArray(), 0.001);
//    assertFalse(it.hasNext());
//    
//    assertFalse(ts.iterator(AlertType.TYPE).isPresent());
//    
//    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
//        ts.iterators();
//    assertEquals(1, its.size());
//    it = its.iterator().next();
//    assertEquals(NumericArrayType.TYPE, it.getType());
//  }
//  
//  @Test
//  public void iteratorWithAlerts() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, RESULTS, "prediction", 
//        "OlympicScoring", BASE_TIME);
//    AlertValue a1 = mock(AlertValue.class);
//    AlertValue a2 = mock(AlertValue.class);
//    AlertValue a3 = mock(AlertValue.class);
//    ts.addAlerts(Lists.newArrayList(a1, a2, a3));
//    
//    TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
//        ts.iterator(NumericArrayType.TYPE).get();
//    assertTrue(it.hasNext());
//    
//    TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) it.next();
//    assertEquals(BASE_TIME.epoch(), v.timestamp().epoch());
//    assertEquals(0, v.value().offset());
//    assertEquals(4, v.value().end());
//    assertFalse(v.value().isInteger());
//    assertArrayEquals(RESULTS, v.value().doubleArray(), 0.001);
//    assertFalse(it.hasNext());
//    
//    it = ts.iterator(AlertType.TYPE).get();
//    assertTrue(it.hasNext());
//    
//    TimeSeriesValue<AlertType> av = (TimeSeriesValue<AlertType>) it.next();
//    assertSame(a1, av);
//    
//    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
//        ts.iterators();
//    assertEquals(2, its.size());
//    it = its.iterator().next();
//    assertEquals(NumericArrayType.TYPE, it.getType());
//  }
//
//  @Test
//  public void itertatorNullResults() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, null, "prediction", 
//        "OlympicScoring", BASE_TIME);
//    TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
//        ts.iterator(NumericArrayType.TYPE).get();
//    assertFalse(it.hasNext());
//    
//    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
//        ts.iterators();
//    assertEquals(1, its.size());
//    it = its.iterator().next();
//    assertEquals(NumericArrayType.TYPE, it.getType());
//  }
//  
//  @Test
//  public void itertatorEmptyResults() throws Exception {
//    EgadsTimeSeries ts = new EgadsTimeSeries(ID, new double[0], "prediction", 
//        "OlympicScoring", BASE_TIME);
//    TypedTimeSeriesIterator<? extends TimeSeriesDataType> it = 
//        ts.iterator(NumericArrayType.TYPE).get();
//    assertFalse(it.hasNext());
//    
//    Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = 
//        ts.iterators();
//    assertEquals(1, its.size());
//    it = its.iterator().next();
//    assertEquals(NumericArrayType.TYPE, it.getType());
//  }
//  
}

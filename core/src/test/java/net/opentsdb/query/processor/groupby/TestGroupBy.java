package net.opentsdb.query.processor.groupby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.DefaultTimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.query.processor.ProcessorTestsHelpers.MockProcessor;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;
import net.opentsdb.query.processor.expressions.JexlBinderNumericIterator;
import net.opentsdb.query.processor.expressions.JexlBinderProcessor;
import net.opentsdb.utils.Exceptions;

public class TestGroupBy {
  private TSDB tsdb;
  private TimeSeriesGroupId group_id;
  
  private TimeSeriesId id_a;
  private TimeSeriesId id_b;
  private TimeSeriesId id_c;
  private TimeSeriesId id_d;
  
  private List<List<MutableNumericType>> data_a;
  private List<List<MutableNumericType>> data_b;
  private List<List<MutableNumericType>> data_c;
  private List<List<MutableNumericType>> data_d;
  
  private Map<String, NumericFillPolicy> fills;
  
  private MockNumericIterator it_a_a;
  private MockNumericIterator it_a_b;
  private MockNumericIterator it_a_c;
  private MockNumericIterator it_a_d;
    
  private GroupByConfig config;
  private TimeSeriesQuery query;
  
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1h-ago")
            .setAggregator("sum"))
        .setFilters(Lists.newArrayList(
            Filter.newBuilder()
            .addFilter(TagVFilter.newBuilder()
                .setTagk("colo")
                .setType("regexp")
                .setGroupBy(true)
                .setFilter(".*"))
            .setId("f1")
            .build()
            ))
        .setMetrics(Lists.newArrayList(
            Metric.newBuilder()
            .setId("m1")
            .setFilter("f1")
            .setMetric("sys.cpu.user")
            .build()
            ))
        .build();
    query.validate();
    
    config = (GroupByConfig) GroupByConfig.newBuilder()
        .setQuery(query)
        .build();
    
    group_id = new SimpleStringGroupId("m1");
    
    id_a = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("sys.cpu.user"))
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    id_b = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("sys.cpu.user"))
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    id_c = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("sys.cpu.user"))
        .addTags("host", "web01")
        .addTags("colo", "phx")
        .build();
    id_d = SimpleStringTimeSeriesId.newBuilder()
        .setMetrics(Lists.newArrayList("sys.cpu.user"))
        .addTags("host", "web02")
        .addTags("colo", "phx")
        .build();
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(3000), 3, 1));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_a, new MillisecondTimeStamp(6000), 6, 1));
    data_a.add(set);

    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(3000), 3, 1));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_b, new MillisecondTimeStamp(6000), 6, 1));
    data_b.add(set);
    
    data_c = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(3000), 3, 1));
    data_c.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_c, new MillisecondTimeStamp(6000), 6, 1));
    data_c.add(set);
    
    data_d = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(1000), 1, 1));
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(2000), 2, 1));
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(3000), 3, 1));
    data_d.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(4000), 4, 1));
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(5000), 5, 1));
    set.add(new MutableNumericType(id_d, new MillisecondTimeStamp(6000), 6, 1));
    data_d.add(set);
    
    it_a_a = new MockNumericIterator(id_a);
    it_a_a.data = data_a;
    it_a_b = new MockNumericIterator(id_b);
    it_a_b.data = data_b;
    
    it_a_c = new MockNumericIterator(id_c);
    it_a_c.data = data_c;
    it_a_d = new MockNumericIterator(id_d);
    it_a_d.data = data_d;
    
    context = spy(new DefaultQueryContext(tsdb, 
        mock(ExecutionGraph.class)));
  }
  
  @Test
  public void foo() throws Exception {
    GroupBy processor = new GroupBy(null, config);
    processor.addSeries(group_id, it_a_a);
    processor.addSeries(group_id, it_a_b);
    processor.addSeries(group_id, it_a_c);
    processor.addSeries(group_id, it_a_d);
    
    processor.initialize().join();
    for (TimeSeriesIterator<?> it : processor.iterators().flattenedIterators()) {
      System.out.println("IT: " + it.id());
      
      while (it.status() != IteratorStatus.END_OF_DATA) {
        while (it.status() == IteratorStatus.HAS_DATA) {
          TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
          System.out.println(v.timestamp().msEpoch() + " " + 
          (v.value().isInteger() ? v.value().longValue() : v.value().doubleValue()));
        }
        if (it.status() == IteratorStatus.END_OF_CHUNK) {
          it.fetchNext().join();
        }
      }
    }
  }
  
//  
//  @Test
//  public void ctor() throws Exception {
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    verify(context, times(1)).register(processor);
//    
//    processor = new JexlBinderProcessor(null, config);
//    verify(context, never()).register(processor);
//    
//    try {
//      new JexlBinderProcessor(context, null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//  
//  @Test
//  public void initialize() throws Exception {
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    processor.addProcessor(group);
//    
//    assertNull(context.initialize().join());
//    assertEquals(2, processor.iterators().flattenedIterators().size());
//    assertTrue(processor.iterators().flattenedIterators().get(0) 
//        instanceof JexlBinderNumericIterator);
//    assertTrue(processor.iterators().flattenedIterators().get(1) 
//        instanceof JexlBinderNumericIterator);
//    
//    final IllegalStateException e = new IllegalStateException("Boo!");
//    
//    context = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
//    MockProcessor mock_proc = new MockProcessor(1, e);
//    processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(mock_proc);
//    try {
//      context.initialize().join();
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException ex) { 
//      assertSame(ex, e);
//    }
//    
//    context = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
//    mock_proc = new MockProcessor(1, e);
//    mock_proc.setThrowException(1);
//    processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(mock_proc);
//    try {
//      context.initialize().join();
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException ex) { 
//      assertSame(ex, e);
//    }
//    
//    context = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
//    mock_proc = new MockProcessor(1, e);
//    mock_proc.setThrowException(2);
//    processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(mock_proc);
//    try {
//      context.initialize().join();
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException ex) { 
//      assertSame(ex, e);
//    }
//  }
//  
//  @Test
//  public void initializeNonNumerics() throws Exception {
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    group = new DefaultTimeSeriesProcessor(context);
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, new MockAnnotationIterator(id_a));
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, new MockAnnotationIterator(id_b));
//    processor.addProcessor(group);
//    
//    assertNull(context.initialize().join());
//    assertEquals(1, processor.iterators().flattenedIterators().size());
//    
//    context = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
//    processor = new JexlBinderProcessor(context, config);
//    group = new DefaultTimeSeriesProcessor(context);
//    group.addSeries(group_id_a, new MockAnnotationIterator(id_a));
//    group.addSeries(group_id_b, new MockAnnotationIterator(id_b));
//    processor.addProcessor(group);
//    
//    assertNull(context.initialize().join());
//    assertEquals(0, processor.iterators().flattenedIterators().size());
//  }
//  
//  @Test
//  public void addSeries() throws Exception {
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    assertTrue(processor.iterators().flattenedIterators().isEmpty());
//    
//    processor.addSeries(group_id_a, it_a_a);
//    assertTrue(processor.iterators().flattenedIterators().isEmpty());
//    verify(context, times(1)).register(it_a_a);
//    
//    try {
//      processor.addSeries(null, it_a_a);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      processor.addSeries(group_id_a, null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//  
//  @Test
//  public void addProcessor() throws Exception {
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    assertTrue(processor.iterators().flattenedIterators().isEmpty());
//    
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    processor.addProcessor(group);
//    assertTrue(processor.iterators().flattenedIterators().isEmpty());
//    verify(context, times(1)).register(processor, group);
//    
//    try {
//      processor.addProcessor(null);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//    
//    try {
//      processor.addProcessor(processor);
//      fail("Expected IllegalArgumentException");
//    } catch (IllegalArgumentException e) { }
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void completeUnion() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(group);
//    assertNull(context.initialize().join());
//    
//    assertEquals(2, processor.iterators().group(
//        new SimpleStringGroupId("e1")).flattenedIterators().size());
//    final List<TimeSeriesIterator<?>> its = processor.iterators().flattenedIterators();
//    assertEquals(2, its.size());
//    long ts = 1000;
//    double value = 2.0;
//    int fetched = 0;
//    
//    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
//      while (context.advance() == IteratorStatus.HAS_DATA) {
//        for (final TimeSeriesIterator<?> it : its) {
//          final TimeSeriesValue<?> v = it.next();
//          assertEquals(ts, v.timestamp().msEpoch());
//          assertEquals(value, 
//              ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);
//          System.out.println("Group: na Alias: " + " " + v.timestamp().msEpoch() + " " + 
//            ((TimeSeriesValue<NumericType>) v).value().toDouble());
//        }
//        ts += 1000;
//        value += 2;
//      }
//      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
//        context.fetchNext().join();
//        fetched++;
//      }
//    }
//    assertEquals(1, fetched);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void inCompleteUnionVariableFill() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    //group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//        
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(group);
//    
//    assertNull(context.initialize().join());
//    
//    final List<TimeSeriesIterator<?>> its = processor.iterators().flattenedIterators();
//    assertEquals(2, its.size());
//    long ts = 1000;
//    double value_a = -99.0;
//    double value_b = 2.0;
//    int fetched = 0;
//    int idx = 0;
//    
//    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
//      while (context.advance() == IteratorStatus.HAS_DATA) {
//        for (final TimeSeriesIterator<?> it : its) {
//          final TimeSeriesValue<?> v = it.next();
//          assertEquals(ts, v.timestamp().msEpoch());
//          if (idx % 2 == 0) {
//            assertEquals(value_a, 
//                ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);
//          } else {
//            assertEquals(value_b, 
//                ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);  
//          }
//          idx++;
//        }
//        ts += 1000;
//        value_a++;
//        value_b += 2;
//      }
//      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
//        context.fetchNext().join();
//        fetched++;
//      }
//    }
//    assertEquals(1, fetched);
//  }
//  
//  @Test
//  public void inCompleteUnionNoVariableFill() throws Exception {
//    expression = Expression.newBuilder()
//        .setId("e1")
//        .setExpression("a + b")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        //.setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//    
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    //group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(group);
//    
//    try {
//      context.initialize().join();
//      fail("Expected DeferredGroupException");
//    } catch (DeferredGroupException e) {
//      assertTrue(Exceptions.getCause(e) instanceof IllegalStateException);
//    }
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void intersection() throws Exception {
//    join = Join.newBuilder()
//        .setOperator(SetOperator.INTERSECTION)
//        .setTags(Lists.newArrayList("host", "colo"))
//        .build();
//    
//    expression = Expression.newBuilder()
//        .setId("e1")
//        .setExpression("a + b")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//    
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    //group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(group);
//    assertNull(context.initialize().join());
//    
//    assertEquals(2, processor.iterators().group(
//        new SimpleStringGroupId("e1")).flattenedIterators().size());
//    final List<TimeSeriesIterator<?>> its = processor.iterators().flattenedIterators();
//    assertEquals(2, its.size());
//    long ts = 1000;
//    double value_a = -99.0;
//    double value_b = 2.0;
//    int idx = 0;
//    int fetched = 0;
//    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
//      while (context.advance() == IteratorStatus.HAS_DATA) {
//        for (final TimeSeriesIterator<?> it : its) {
//          final TimeSeriesValue<?> v = it.next();
//          assertEquals(ts, v.timestamp().msEpoch());
//          if (idx % 2 == 0) {
//            assertEquals(value_a, 
//                ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);
//          } else {
//            assertEquals(value_b, 
//                ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);  
//          }
//          idx++;
//          System.out.println("Group: na Alias: " + " " + v.timestamp().msEpoch() + " " + 
//            ((TimeSeriesValue<NumericType>) v).value().toDouble());
//        }
//        ts += 1000;
//        value_a++;
//        value_b += 2;
//      }
//      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
//        context.fetchNext().join();
//        fetched++;
//      }
//    }
//    assertEquals(1, fetched);
//  }
//
//  @SuppressWarnings("unchecked")
//  @Test
//  public void nested() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    JexlBinderProcessor p1 = new JexlBinderProcessor(context, config);
//    p1.addProcessor(group);
//
//    it_a_a = new MockNumericIterator(id_a);
//    it_a_a.data = data_a;
//    it_a_b = new MockNumericIterator(id_b);
//    it_a_b.data = data_b;
//    
//    it_b_a = new MockNumericIterator(id_a);
//    it_b_a.data = data_a;
//    it_b_b = new MockNumericIterator(id_b);
//    it_b_b.data = data_b;
//    
//    group = new DefaultTimeSeriesProcessor(context);
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    expression = Expression.newBuilder()
//        .setId("e2")
//        .setExpression("a + b")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor p2 = new JexlBinderProcessor(context, config);
//    p2.addProcessor(group);
//    
//    expression = Expression.newBuilder()
//        .setId("e3")
//        .setExpression("e1 * e2")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(p1);
//    processor.addProcessor(p2);
//    
//    assertNull(context.initialize().join());
//
//    assertEquals(2, processor.iterators().group(
//        new SimpleStringGroupId("e3")).flattenedIterators().size());
//    final List<TimeSeriesIterator<?>> its = processor.iterators().flattenedIterators();
//    assertEquals(2, its.size());
//    long ts = 1000;
//    double value = 2.0;
//    int fetched = 0;
//    
//    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
//      while (context.advance() == IteratorStatus.HAS_DATA) {
//        for (final TimeSeriesIterator<?> it : its) {
//          final TimeSeriesValue<?> v = it.next();
//          assertEquals(ts, v.timestamp().msEpoch());
//          assertEquals(value * value, 
//              ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);
//        }
//        ts += 1000;
//        value += 2;
//      }
//      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
//        context.fetchNext().join();
//        fetched++;
//      }
//    }
//
//    assertEquals(1, fetched);
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void nestedWithIterators() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    JexlBinderProcessor p1 = new JexlBinderProcessor(context, config);
//    p1.addProcessor(group);
//
//    expression = Expression.newBuilder()
//        .setId("e2")
//        .setExpression("e1 / c")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(p1);
//    it_a_a = new MockNumericIterator(id_a);
//    it_a_a.data = data_a;
//    it_a_b = new MockNumericIterator(id_b);
//    it_a_b.data = data_b;
//    processor.addSeries(new SimpleStringGroupId("c"), it_a_a);
//    processor.addSeries(new SimpleStringGroupId("c"), it_a_b);
//    
//    assertNull(context.initialize().join());
//
//    assertEquals(2, processor.iterators().group(
//        new SimpleStringGroupId("e2")).flattenedIterators().size());
//    final List<TimeSeriesIterator<?>> its = processor.iterators().flattenedIterators();
//    assertEquals(2, its.size());
//    long ts = 1000;
//    int fetched = 0;
//    
//    while (context.nextStatus() != IteratorStatus.END_OF_DATA) {
//      while (context.advance() == IteratorStatus.HAS_DATA) {
//        for (final TimeSeriesIterator<?> it : its) {
//          final TimeSeriesValue<?> v = it.next();
//          assertEquals(ts, v.timestamp().msEpoch());
//          assertEquals(2.0, 
//              ((TimeSeriesValue<NumericType>) v).value().doubleValue(), 0.001);
//        }
//        ts += 1000;
//      }
//      if (context.currentStatus() == IteratorStatus.END_OF_CHUNK) {
//        context.fetchNext().join();
//        fetched++;
//      }
//    }
//
//    assertEquals(1, fetched);
//  }
//  
//  @Test
//  public void nestedWithIteratorsOneMissing() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    JexlBinderProcessor p1 = new JexlBinderProcessor(context, config);
//    p1.addProcessor(group);
//
//    expression = Expression.newBuilder()
//        .setId("e2")
//        .setExpression("e1 / c")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(p1);
//    it_a_a = new MockNumericIterator(id_a);
//    it_a_a.data = data_a;
//    //it_a_b = new MockNumericIterator(id_b);
//    //it_a_b.data = data_b;
//    processor.addSeries(new SimpleStringGroupId("c"), it_a_a);
//    //processor.addSeries(new SimpleStringGroupId("c"), it_a_b);
//    
//    try {
//      context.initialize().join();
//      fail("Expected DeferredGroupException");
//    } catch (DeferredGroupException e) { }
//  }
//  
//  @Test
//  public void nestedCycle() throws Exception {
//    expression = Expression.newBuilder()
//        .setId("e1")
//        .setExpression("e3 + a")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    JexlBinderProcessor p1 = new JexlBinderProcessor(context, config);
//    p1.addProcessor(group);
//
//    it_a_a = new MockNumericIterator(id_a);
//    it_a_a.data = data_a;
//    it_a_b = new MockNumericIterator(id_b);
//    it_a_b.data = data_b;
//    
//    it_b_a = new MockNumericIterator(id_a);
//    it_b_a.data = data_a;
//    it_b_b = new MockNumericIterator(id_b);
//    it_b_b.data = data_b;
//    
//    group = new DefaultTimeSeriesProcessor(context);
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    expression = Expression.newBuilder()
//        .setId("e2")
//        .setExpression("a + b")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor p2 = new JexlBinderProcessor(context, config);
//    p2.addProcessor(group);
//    
//    expression = Expression.newBuilder()
//        .setId("e3")
//        .setExpression("e1 * e2")
//        .setFillPolicy(NumericFillPolicy.newBuilder()
//            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
//        .setFillPolicies(fills)
//        .setJoin(join)
//        .build();
//
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression)
//        .build();
//    
//    JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(p1);
//    processor.addProcessor(p2);
//    
//    try {
//      p1.addProcessor(processor);
//      fail("Expected IllegalStateException");
//    } catch (IllegalStateException e) { }
//  }
//
//  @Test
//  public void getClone() throws Exception {
//    group.addSeries(group_id_a, it_a_a);
//    group.addSeries(group_id_a, it_a_b);
//    group.addSeries(group_id_b, it_b_a);
//    group.addSeries(group_id_b, it_b_b);
//    
//    final JexlBinderProcessor processor = new JexlBinderProcessor(context, config);
//    processor.addProcessor(group);
//    assertNull(context.initialize().join());
//    assertEquals(2, processor.iterators().flattenedIterators().size());
//    
//    assertEquals(IteratorStatus.HAS_DATA, context.advance());
//    TimeSeriesValue<?> v = processor.iterators()
//        .flattenedIterators().get(0).next();
//    assertEquals(1000, v.timestamp().msEpoch());
//    v = processor.iterators().flattenedIterators().get(1).next();
//    assertEquals(1000, v.timestamp().msEpoch());
//    assertEquals(IteratorStatus.HAS_DATA, context.advance());
//    
//    final QueryContext ctx2 = new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
//    final JexlBinderProcessor clone = (JexlBinderProcessor) processor.getClone(ctx2);
//    assertNull(ctx2.initialize().join());
//    
//    assertNotSame(processor, clone);
//    assertEquals(2, clone.iterators().flattenedIterators().size());
//    assertNotSame(processor.iterators().flattenedIterators().get(0), 
//        clone.iterators().flattenedIterators().get(0));
//    assertNotSame(processor.iterators().flattenedIterators().get(1), 
//        clone.iterators().flattenedIterators().get(1));
//    
//    assertEquals(IteratorStatus.HAS_DATA, ctx2.advance());
//    v = clone.iterators().flattenedIterators().get(0).next();
//    assertEquals(1000, v.timestamp().msEpoch());
//    v = clone.iterators().flattenedIterators().get(1).next();
//    assertEquals(1000, v.timestamp().msEpoch());
//  }

}

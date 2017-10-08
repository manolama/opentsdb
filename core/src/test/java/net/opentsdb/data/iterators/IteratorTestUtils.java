// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.junit.Ignore;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericMillisecondShard2;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;

/**
 * A helper class with static utilities for working with iterators in unit
 * tests.
 */
@Ignore
public class IteratorTestUtils {

  public static TimeSeriesId ID_A = BaseTimeSeriesId.newBuilder()
      .setMetric("system.cpu.user")
      .build();
  public static TimeSeriesId ID_B = BaseTimeSeriesId.newBuilder()
      .setMetric("system.cpu.idle")
      .build();
  
  public static TimeSeriesGroupId GROUP_A = new SimpleStringGroupId("a");
  public static TimeSeriesGroupId GROUP_B = new SimpleStringGroupId("b");
  
  /**
   * Generates an {@link IteratorGroups} with two groups with two time series
   * each including real values from the start to end timestamp.
   * @param start A start timestamp in milliseconds.
   * @param end An end timestamp in milliseconds.
   * @param order The order of the results.
   * @param interval The interval between values in milliseconds.
   * @return An IteratorGroups object.
   */
  public static IteratorGroups generateData(final long start, 
                                            final long end, 
                                            final int order, 
                                            final long interval) {
    final IteratorGroups groups = new DefaultIteratorGroups();
    
    NumericMillisecondShard shard = new NumericMillisecondShard(ID_A, 
        new MillisecondTimeStamp(start), new MillisecondTimeStamp(end), order);
    for (long ts = start; ts <= end; ts += interval) {
      shard.add(ts, ts);
    }
    
    groups.addIterator(GROUP_A, shard);
    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
    
    shard = new NumericMillisecondShard(ID_B, 
        new MillisecondTimeStamp(start), new MillisecondTimeStamp(end), order);
    for (long ts = start; ts <= end; ts += interval) {
      shard.add(ts, ts);
    }
    
    groups.addIterator(GROUP_A, shard);
    groups.addIterator(GROUP_B, shard.getShallowCopy(null));
    
    return groups;
  }
  
  public static QueryResult generateData(final long start, final long end, final long interval) {
    class Result implements QueryResult {

      @Override
      public TimeSpecification timeSpecification() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Collection<TimeSeries> timeSeries() {
        
        class TS implements TimeSeries {
          final TimeSeriesId id;
          final NumericMillisecondShard2 shard;
          public TS(final TimeSeriesId id) {
            this.id = id;
            shard = new NumericMillisecondShard2(
                new MillisecondTimeStamp(start), new MillisecondTimeStamp(end));
            for (long ts = start; ts <= end; ts += interval) {
              shard.add(ts, ts);
            }
          }
          
          @Override
          public TimeSeriesId id() {
            return id;
          }

          @Override
          public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
              final TypeToken<?> type) {
            if (type == NumericType.TYPE) {
              return Optional.of(shard.iterator());
            }
            return Optional.empty();
          }

          @Override
          public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
            return Lists.newArrayList(shard.iterator());
          }

          @Override
          public Collection<TypeToken<?>> types() {
            return Lists.newArrayList(NumericType.TYPE);
          }

          @Override
          public void close() {
            // TODO Auto-generated method stub
            
          }
          
        }
        
        final List<TimeSeries> series = Lists.newArrayListWithCapacity(2);
        series.add(new TS(ID_A));
        series.add(new TS(ID_B));
        return series;
      }

      @Override
      public int sequenceId() {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public QueryNode source() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }
    return new Result();
  }

  @SuppressWarnings("unchecked")
  public static void validateData(final IteratorGroups results, 
                                  final long start, 
                                  final long end,  
                                  final int order, 
                                  final long interval) {
    assertEquals(4, results.flattenedIterators().size());
    IteratorGroup group = results.group(IteratorTestUtils.GROUP_A);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    TimeSeriesIterator<NumericType> iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    long ts = start;
    int count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    group = results.group(IteratorTestUtils.GROUP_B);
    assertEquals(2, group.flattenedIterators().size());
    assertSame(IteratorTestUtils.ID_A, group.flattenedIterators().get(0).id());
    assertSame(IteratorTestUtils.ID_B, group.flattenedIterators().get(1).id());
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(0);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    iterator = 
        (TimeSeriesIterator<NumericType>) group.flattenedIterators().get(1);
    assertEquals(start, iterator.startTime().msEpoch());
    assertEquals(end, iterator.endTime().msEpoch());
    ts = start;
    count = 0;
    while (iterator.status() == IteratorStatus.HAS_DATA) {
      TimeSeriesValue<NumericType> v = iterator.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
  }
  
  @SuppressWarnings("unchecked")
  public static void validateData(final QueryResult results, 
                                  final long start, 
                                  final long end,  
                                  final int order, 
                                  final long interval) {
    assertEquals(2, results.timeSeries().size());
    
    final Iterator<TimeSeries> series_iterator = results.timeSeries().iterator();
    TimeSeries series = series_iterator.next();
    assertSame(IteratorTestUtils.ID_A, series.id());
    
    long ts = start;
    int count = 0;
    Iterator<TimeSeriesValue<? extends TimeSeriesDataType>> it = 
        series.iterator(NumericType.TYPE).get();
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
    
    series = series_iterator.next();
    assertSame(IteratorTestUtils.ID_B, series.id());
    
    ts = start;
    count = 0;
    it = series.iterator(NumericType.TYPE).get();
    while (it.hasNext()) {
      TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
      assertEquals(ts, v.timestamp().msEpoch());
      assertEquals(ts, v.value().longValue());
      ts += interval;
      ++count;
    }
    assertEquals(((end - start) / interval) + 1, count);
  }
}
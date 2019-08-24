// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.execution.cache;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.utils.Pair;

public class CombinedArray implements TypedTimeSeriesIterator<NumericArrayType>, 
    TimeSeriesValue<NumericArrayType>, NumericArrayType {
  
  protected int idx = 0;
  protected boolean called = false;
  protected long[] long_array;
  protected double[] double_array;
  protected final CombinedResult result;
  
  CombinedArray(final CombinedResult result, 
                final List<Pair<QueryResult, TimeSeries>> series) {
    this.result = result;
    final int array_length = (int) ((result.timeSpecification().end().epoch() - 
        result.timeSpecification().start().epoch()) /
        result.timeSpecification().interval().get(ChronoUnit.SECONDS));
    System.out.println("          ARRAY LEN: " + array_length);
    TimeStamp timestamp = result.timeSpecification().start().getCopy();
    timestamp.snapToPreviousInterval(result.resultInterval(), result.resultUnits());
    
    final long interval_in_seconds = result.timeSpecification().interval().get(ChronoUnit.SECONDS);
    long next_epoch = timestamp.epoch();
    for (int i = 0; i < series.size(); i++) {
      final TimeSpecification series_spec = series.get(i).getKey().timeSpecification();
      System.out.println("  WORKING : " + series_spec.start().epoch() + "  EXPECT: " + next_epoch);
      
      final TypedTimeSeriesIterator<NumericArrayType> iterator = 
          (TypedTimeSeriesIterator<NumericArrayType>) 
            series.get(i).getValue().iterator(NumericArrayType.TYPE).get();
      final TimeSeriesValue<NumericArrayType> value = iterator.next();
      
      while (next_epoch != series_spec.start().epoch()) {
        // fill
        System.out.println("     [[[[[[ FILLING ]]]]]]  " + (result.timeSpecification().start().epoch() - next_epoch));
        if (double_array == null) {
          double_array = new double[array_length];
          Arrays.fill(double_array, Double.NaN);
          if (long_array != null) {
            // flip
            for (int x = 0; x < idx; x++) {
              double_array[x] = long_array[x];
            }
            long_array = null;
          }
        }
        
        // edge case if the query is not aligned to the cache interval
        if (result.timeSpecification().start().epoch() > next_epoch) {
          System.out.println("HERE!");
          idx += (((next_epoch + (series_spec.end().epoch() - series_spec.start().epoch())) - result.timeSpecification().start().epoch()) / interval_in_seconds);
        } else {
          idx += ((series_spec.end().epoch() - series_spec.start().epoch()) / interval_in_seconds);
        }
        System.out.println("    FILL IDX: " + idx);
        next_epoch += series_spec.end().epoch() - series_spec.start().epoch();
        if (next_epoch > result.timeSpecification().end().epoch()) {
          throw new IllegalStateException("Coding bug, please report this query.");
        }
      }
      
      int start_offset = result.timeSpecification().start().epoch() > series_spec.start().epoch() ?
          (int) (value.value().offset() + (result.timeSpecification().start().epoch() - series_spec.start().epoch()) / interval_in_seconds)
          : value.value().offset();
      int end = series_spec.end().compare(Op.GT, result.timeSpecification().end()) ?
          (int) (value.value().end() - (series_spec.end().epoch() - result.timeSpecification().end().epoch()) / interval_in_seconds) - start_offset
          : value.value().end() - start_offset;
      System.out.println(" [SO] " + start_offset + " [E] " + end);
      // we have some data to write.
      if (value.value().isInteger()) {
        if (long_array == null && double_array == null) {
          if (start_offset > 0) {
            // we're offset so we need to fill
            double_array = new double[array_length];
            Arrays.fill(double_array, Double.NaN);
            for (int x = start_offset; x < end; x++) {
              double_array[idx++] = value.value().longArray()[x];
            }
          } else {
            // start with a long array
            long_array = new long[array_length];
            System.arraycopy(value.value().longArray(), start_offset, long_array, idx, end);
            idx += end;
          }
        } else if (double_array != null) {
          for (int x = start_offset; x < end; x++) {
            double_array[idx++] = value.value().longArray()[x];
          }
        } else {
          System.arraycopy(value.value().longArray(), start_offset, long_array, idx, end);
          idx += end;
        }
      } else {
        if (double_array == null) {
          // flip
          double_array = new double[array_length];
          Arrays.fill(double_array, Double.NaN);
          for (int x = 0; x < idx; x++) {
            double_array[x] = long_array[x];
          }
        }
        System.arraycopy(value.value().doubleArray(), start_offset, double_array, idx, end);
        idx += end;
      }
      series.get(i).getValue().close();
      next_epoch += series_spec.end().epoch() - series_spec.start().epoch();
      System.out.println("     [EPOCH] " + next_epoch);
    }
    
    // adjust in case we were missing data at the end of the interval.
    if (long_array != null) {
      idx = long_array.length;
    } else {
      idx = double_array.length;
    }
    System.out.println("DONE.........: " + idx);
  }

  @Override
  public boolean hasNext() {
    return !called;
  }

  @Override
  public TimeSeriesValue<NumericArrayType> next() {
    called = true;
    return this;
  }

  @Override
  public TypeToken<NumericArrayType> getType() {
    return NumericArrayType.TYPE;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int end() {
    return idx;
  }

  @Override
  public boolean isInteger() {
    return long_array != null;
  }

  @Override
  public long[] longArray() {
    return long_array;
  }

  @Override
  public double[] doubleArray() {
    return double_array;
  }

  @Override
  public TypeToken<NumericArrayType> type() {
    return NumericArrayType.TYPE;
  }

  @Override
  public TimeStamp timestamp() {
    return result.timeSpecification().start();
  }

  @Override
  public NumericArrayType value() {
    return this;
  }
}

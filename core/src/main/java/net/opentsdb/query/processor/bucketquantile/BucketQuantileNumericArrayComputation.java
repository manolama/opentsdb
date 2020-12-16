package net.opentsdb.query.processor.bucketquantile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.processor.bucketquantile.BucketQuantile.Bucket;

public class BucketQuantileNumericArrayComputation extends BucketQuantileComputer {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketQuantileNumericArrayComputation.class);
  final QueryNode node;
  final TimeSeries[] sources;
  double[][] percentiles;
  TimeStamp timestamp;
  // TODO - byte IDs
  TimeSeriesId id;
  
  BucketQuantileNumericArrayComputation(final int index,
                                        final QueryNode node,
                                        final TimeSeries[] sources) {
    super(index);
    this.node = node;
    this.sources = sources;
  }
  
  void run() {
    long[] accumulator = new long[sources.length];
    long[][] results = new long[sources.length][];
    int[] indices = new int[sources.length];
    List<Double> ptiles = ((BucketQuantileConfig) node.config()).getQuantiles();
    percentiles = new double[ptiles.size()][];
    
    Bucket overflow = ((BucketQuantile) node).buckets()[((BucketQuantile) node).buckets().length - 1];
    double[] p_thresholds = new double[percentiles.length];
    int limit = -1;
    for (int i = 0; i < sources.length; i++) {
      if (sources[i] == null) {
        indices[i] = -1;
        LOG.debug("No source at " + i);
        continue;
      }
      
      final Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> op = 
          sources[i].iterator(NumericArrayType.TYPE);
      if (!op.isPresent()) {
        indices[i] = -1;
        LOG.debug("No array iterator for source at " + i + ": " + sources[i].id());
        continue;
      }
      
      final TypedTimeSeriesIterator<? extends TimeSeriesDataType> iterator = op.get();
      if (!iterator.hasNext()) {
        indices[i] = -1;
        LOG.debug("No data for the iterator for source at " + i + ": " 
            + sources[i].id());
        continue;
      }
      
      final TimeSeriesValue<NumericArrayType> value = 
          (TimeSeriesValue<NumericArrayType>) iterator.next();
      if (value.value() == null) {
        indices[i] = -1;
        LOG.debug("Null value from source at " + i + ": " + sources[i].id());
        continue;
      }
      
      if (timestamp == null) {
        timestamp = value.timestamp().getCopy();
        id = sources[i].id();
      }
      indices[i] = value.value().offset();
      if (limit < 0) {
        // Better be the same for all.
        limit = value.value().end() - value.value().offset();
      } else if (value.value().end() - value.value().offset() != limit) {
        throw new IllegalStateException("The size of the array from the source "
            + "at " + i + ": " + sources[i].id() + " was less than the "
                + "limit of [" + limit + "] => " + (value.value().end() - 
                    value.value().offset()));
      }
      
      if (value.value().isInteger()) {
        results[i] = value.value().longArray();
      } else {
        // TODO - ewwwww, eww! No!! This shouldn't happen because histograms
        // are counts!
        LOG.debug("Converting from double to long at " + i + ": " + sources[i].id());
        long[] new_value = new long[value.value().end() - value.value().offset()];
        double[] double_value = value.value().doubleArray();
        for (int x = value.value().offset(); x < value.value().end(); x++) {
          new_value[x] = (long) double_value[x];
        }
        results[i] = new_value;
      }
    }
    
    // setup the ptiles
    for (int i = 0; i < percentiles.length; i++) {
      // TODO - pools
      percentiles[i] = new double[limit];
    }
    
    // now that we have all of the counts in order we can start the iteration.
    for (int i = 0; i < limit; i++) {
      // load the accumulator
      // TODO - overflow
      long sum = -1;
      for (int x = 0; x < indices.length; x++) {
        if (indices[x] < 0) {
          // null
          accumulator[x] = -1;
        } else {
          accumulator[x] = results[x][indices[x]++];
          if (sum < 0) {
            sum = accumulator[x];
          } else {
            sum += accumulator[x];
          }
        }
      }
      
      // now process.
      //System.out.println("           SUM: " + sum);
      if (sum < 0) {
        for (int y = 0; y < percentiles.length; y++) {
          percentiles[y][i] = Double.NaN;
        }
        continue;
      }
      
      // ptiles are sorted so we keep counting until we exceed the ptile, then
      // advance.
      for (int y = 0; y < ptiles.size(); y++) {
        p_thresholds[y] = ((double) sum) * ptiles.get(y);
      }
      
      int threshold_idx = 0;
      double so_far = -1;
      for (int z = 0; z < accumulator.length; z++) {
        if (accumulator[z] < 0) {
          continue;
        }
        
        so_far += accumulator[z];
        while (so_far >= p_thresholds[threshold_idx]) {
          percentiles[threshold_idx][i] = 
              ((BucketQuantile) node).buckets()[z].report;
          p_thresholds[threshold_idx] = -Double.MIN_VALUE;
          threshold_idx++;
          
          if (threshold_idx >= p_thresholds.length) {
            // done, no more work to do.
            break;
          }
        }
      }
      
      if (so_far < 0) {
        for (int z = 0; z < ptiles.size(); z++) {
          percentiles[z][i] = Double.NaN;
        }
      } else {
        // max
        for (int z = 0; z < ptiles.size(); z++) {
          if (p_thresholds[z] != -Double.MIN_VALUE) {
            percentiles[z][i] = overflow.report;
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  TimeSeries getSeries(final int percentile_index) {
    return new BucketQuantileNumericArrayIterator(timestamp,
        percentiles[percentile_index],
        percentiles[percentile_index].length,
        id, 
        ((BucketQuantileConfig) node.config()).getAs(),
        ((BucketQuantileConfig) node.config()).getQuantiles().get(percentile_index));
  }
}

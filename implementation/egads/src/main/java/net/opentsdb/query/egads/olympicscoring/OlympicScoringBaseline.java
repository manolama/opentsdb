package net.opentsdb.query.egads.olympicscoring;

import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.reflect.TypeToken;
import com.yahoo.egads.models.tsmm.OlympicModel2;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.egads.EgadsTimeSeries;

public class OlympicScoringBaseline {
  public static final String MODEL_TAG_VALUE = "OlympicScoring";
  
  private static final Logger LOG = LoggerFactory.getLogger(
      OlympicScoringBaseline.class);
  
  protected final OlympicScoringNode node;
  protected final TimeSeriesId id;
  protected final com.yahoo.egads.data.TimeSeries baseline;
  protected long last_ts;
  
  OlympicScoringBaseline(final OlympicScoringNode node, 
                         final TimeSeriesId id) {
    this.node = node;
    this.id = id;
    baseline = new com.yahoo.egads.data.TimeSeries();
  }
  
  /**
   * Assume that we get time series in order from oldest to newest.
   * @param series
   * @param result
   */
  void append(final TimeSeries series, final QueryResult result) {
    TypeToken<? extends TimeSeriesDataType> type = null;
    for (final TypeToken<? extends TimeSeriesDataType> t : series.types()) {
      if (t == NumericType.TYPE ||
          t == NumericArrayType.TYPE ||
          t == NumericSummaryType.TYPE) {
        type = t;
        break;
      }
    }
    if (type == null) {
      LOG.warn("Time series: " + series.id() + " did not have a supported "
          + "numeric iterator.");
      return;
    }
    
    final Optional<TypedTimeSeriesIterator<?>> optional = series.iterator(type);
    if (!optional.isPresent()) {
      LOG.warn("Time series: " + series.id() + " did not have a supported "
          + "numeric iterator.");
      return;
    }
    
    if (type == NumericType.TYPE) {
      processNumeric(optional.get());
    } else if (type == NumericArrayType.TYPE) {
      processNumericArray(optional.get(), result);
    } else if (type == NumericSummaryType.TYPE) {
      processSummary(optional.get());
    }
  }

  TimeSeries predict(final Properties properties) {
    if (baseline.size() < 2) {
      LOG.warn("Not enough data points to predict: " + baseline.size());
      return null;
    }
    
    final com.yahoo.egads.data.TimeSeries prediction = 
        new com.yahoo.egads.data.TimeSeries();
    final double[] results = new double[(int) (node.predictionWidth() / 
        node.predictionInterval())];
    
    // fill the prediction with nans at the proper timestamps
    long ts = node.predictionStart();
    for (int i = 0; i < results.length; i++) {
      try {
        prediction.append(ts, Float.NaN);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      ts += node.predictionInterval();
    }
    
    // wrote the data to the baseline, now train it.
    final OlympicModel2 tsmm = new OlympicModel2(properties);
    try {
      tsmm.train(baseline.data);
      tsmm.predict(prediction.data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    // trained, now populate the query result
    final Iterator<com.yahoo.egads.data.TimeSeries.Entry> it = 
        prediction.data.iterator();
    int i = 0;
    ts = node.predictionStart();
    while (it.hasNext()) {
      com.yahoo.egads.data.TimeSeries.Entry entry = it.next();
      if (entry.time != ts) {
        LOG.warn("Prediction entry (" + entry.time + ", " + entry.value 
            + ") didn't match the expected timestamp " + ts);
        continue;
      }
      results[i++] = entry.value;
      ts += node.predictionInterval();
    }
    
    return new EgadsTimeSeries(id, 
                               results, 
                               "prediction", 
                               MODEL_TAG_VALUE, 
                               new SecondTimeStamp(node.prediction_start));
  }

  void processNumeric(final TypedTimeSeriesIterator iterator) {
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericType> value = 
          (TimeSeriesValue<NumericType>) iterator.next();
      if (value.value() == null) {
        continue;
      }
      
      if (value.timestamp().epoch() > last_ts) {
        if (!Double.isNaN(value.value().toDouble())) {
          try {
            baseline.append(value.timestamp().epoch(), 
                (float) value.value().toDouble());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        last_ts = value.timestamp().epoch();
      }
    }
  }
  
  void processNumericArray(final TypedTimeSeriesIterator iterator, 
                           final QueryResult result) {
    final TimeSeriesValue<NumericArrayType> value = 
        (TimeSeriesValue<NumericArrayType>) iterator.next();
    if (value.value() == null) {
      return;
    }
    
    final TimeStamp ts = result.timeSpecification().start();
    if (value.value().isInteger()) {
      final long[] array = value.value().longArray();
      for (int x = value.value().offset(); x < value.value().end(); x++) {
        if (ts.epoch() > last_ts) {
          try {
            baseline.append(ts.epoch(), array[x]);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          last_ts = ts.epoch();
        }
        ts.add(result.timeSpecification().interval());
      }
    } else {
      final double[] array = value.value().doubleArray();
      for (int x = value.value().offset(); x < value.value().end(); x++) {
        // TODO  - ugg!! EGADs needs to support double precision!
        if (Double.isNaN(array[x])) {
          ts.add(result.timeSpecification().interval());
          continue;
        } else if (ts.epoch() <= last_ts) {
          ts.add(result.timeSpecification().interval());
          continue;
        }
        
        try {
          System.out.println("ADD: " + ts.epoch() + "  " + array[x]);
          baseline.append(ts.epoch(), (float) array[x]);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        last_ts = ts.epoch();
        ts.add(result.timeSpecification().interval());
      }
    }
  }
  
  void processSummary(final TypedTimeSeriesIterator iterator) {
    // TODO - just grabbing the first sumary in the set.
    int summary = -1;
    while (iterator.hasNext()) {
      final TimeSeriesValue<NumericSummaryType> value = 
          (TimeSeriesValue<NumericSummaryType>) iterator.next();
      if (value.value() == null) {
        continue;
      }
      
      if (value.timestamp().epoch() > last_ts) {
        if (summary < 0) {
          summary = value.value().summariesAvailable().iterator().next();
        }
        
        NumericType v = value.value().value(summary);
        if (v == null) {
          continue;
        }
        
        if (!Double.isNaN(v.toDouble())) {
          try {
            baseline.append(value.timestamp().epoch(), (float) v.toDouble());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        last_ts = value.timestamp().epoch();
      }
    }
  }
}
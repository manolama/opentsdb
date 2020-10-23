package net.opentsdb.storage;

import java.io.IOException;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.ArrayAggregatorConfig;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.MergedTimeSeriesId;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.aggregators.DefaultArrayAggregatorConfig;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregator;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.pools.ArrayObjectPool;
import net.opentsdb.pools.DoubleArrayPool;
import net.opentsdb.pools.LongArrayPool;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.PooledObject;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.query.processor.groupby.GroupByTimeSeries;
import net.opentsdb.query.processor.rate.RateConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.storage.schemas.tsdb1x.NumericRowSeq;
import net.opentsdb.storage.schemas.tsdb1x.NumericSummaryRowSeq;
import net.opentsdb.storage.schemas.tsdb1x.RowSeq;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.TSUID;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.XXHash;

/**
 * WARNING!!!!!!!!! this sucker is all messed up due to a bad choice earlier.
 * The timestamps are part of the salt hashing SO that means we can have mutltiple
 * threads operating on the same segment at the same time. Thus we need a global
 * map per time series. The segments can come in out of order as well. foo.
 * 
 * TO keep locking and logic simpler, we only allow downsamples that merge evenly
 * and completely into a segment. E.g. if a row is 1 hour in hbase, we allow 1m, 15m
 * and 30m downsampling but NOT 45m or 7m, etc.
 * 
 * FOR configs wherein all of the series hash the same (timestamp is NOT included)
 * then we can use thread local collections that are merged at the end. For this
 * we cannot.
 */
public class TimeHashedDSGBResult extends Tsdb1xQueryResult implements TimeSpecification {
  private static final Logger LOG = LoggerFactory.getLogger(TimeHashedDSGBResult.class);
  
  private static final double[] IDENTITY = {0.0, 0.0, Double.MAX_VALUE, -Double.MAX_VALUE, 0.0};
  private static final ThreadLocal<double[]> threadLocalAggs = ThreadLocal.withInitial(() -> Arrays.copyOf(IDENTITY, IDENTITY.length));
  
  public static enum Agg {
    SUM,
    AVG,
    COUNT,
    MIN,
    MAX,
    LAST,
    NON_OPTIMIZED
  }
  
  private static final byte NUMERIC_TYPE = (byte) 1;
  private static final byte NUMERIC_PREFIX = (byte) 0;
  
  RollupInterval rollup_interval;
  private int storage_interval;
  private final GroupByConfig gbConfig;
  private final DownsampleConfig downsampleConfig;
  private NumericAggregator nonOptimizedAggregator;
//  private PooledObject nonOptimizedPooled;
//  private double[] nonOptimizedArray;
//  private int nonOptimizedIndex;
//  private MutableNumericValue nonOptimizedDp;
//  private PooledObject nonGroupByPooled;
//  private double[] nonGroupByResults;
  //private int intervalIndex;
  private final int startTime;
  private final int endTime;
  private final int interval;
  private final Agg aggregator;
  private final boolean infectiousNans;
  protected final boolean reporting_average;
  private ThreadLocal<Accumulator> threadLocalAccs;
  final NumericArrayAggregatorFactory factory;
  final ArrayAggregatorConfig aggregatorConfig;
  private final ThreadLocal<State> state;
  Set<State> states;
  
//  TLongObjectMap<Foo>[] buckets;
//  TLongObjectMap<GBTS> containers;
  
  List<TimeSeries> final_list;
  
  /**
   * Default ctor.
   * @param sequence_id The sequence ID.
   * @param node The non-null parent node.
   * @param schema The non-null schema.
   */
  public TimeHashedDSGBResult(final QueryNode node, 
                         final Schema schema,
                         final GroupByConfig gb_config,
                         final DownsampleConfig ds_config) {
    super(0, node, schema);
    
    this.gbConfig = gb_config;
    this.downsampleConfig = ds_config;
    states = Sets.newConcurrentHashSet();
    state = ThreadLocal.withInitial(() -> { 
      State s = new State(); 
      states.add(s);
      return s;});
    storage_interval = 3600; // TODO - rollup or 
    factory = node.pipelineContext()
            .tsdb()
            .getRegistry()
            .getPlugin(NumericArrayAggregatorFactory.class, gbConfig.getAggregator());
    if (factory == null) {
      throw new IllegalArgumentException(
          "No numeric array aggregator factory found for type: " + gbConfig.getAggregator());
    }
    //LOG.info("*************** DS INTERVALS: " + downsampleConfig.intervals() + "  DS INTERVAL: " + downsampleConfig.interval());
    aggregatorConfig = DefaultArrayAggregatorConfig.newBuilder()
        .setArraySize(downsampleConfig.intervals())
        .setInfectiousNaN(gbConfig.getInfectiousNan())
        .build();
    
    String agg = downsampleConfig.getAggregator().toLowerCase();
    if (agg.equalsIgnoreCase("AVG") && downsampleConfig.dpsInInterval() > 0) {
      reporting_average = true;
      agg = "sum";
    } else {
      reporting_average = false;
    }
    
    switch (agg) {
    case "sum":
    case "zimsum":
      aggregator = Agg.SUM;
      break;
    case "avg":
      aggregator = Agg.AVG;
      break;
    case "count":
      aggregator = Agg.COUNT;
      break;
    case "max":
    case "mimmax":
      aggregator = Agg.MAX;
      break;
    case "min":
    case "mimmin":
      aggregator = Agg.MIN;
      break;
    case "last":
      aggregator = Agg.LAST;
      break;
    default:
      aggregator = Agg.NON_OPTIMIZED;
      final NumericAggregatorFactory fact = node.pipelineContext().tsdb().getRegistry()
          .getPlugin(NumericAggregatorFactory.class, agg);
      nonOptimizedAggregator = fact.newAggregator(downsampleConfig.getInfectiousNan());

    }
    
    this.startTime = (int) downsampleConfig.startTime().epoch();
    this.endTime = (int) downsampleConfig.endTime().epoch();
    this.interval =
        downsampleConfig.getRunAll()
            ? (endTime - startTime)
            : (int) downsampleConfig.interval().get(ChronoUnit.SECONDS);
    
    this.infectiousNans = downsampleConfig.getInfectiousNan();
    threadLocalAccs = ThreadLocal.withInitial(() -> new Accumulator(3600));
    
    // reduce locking a tiny bit
//    buckets = new TLongObjectMap[16];
//    for (int i = 0; i < 16; i++) {
//      buckets[i] = new TLongObjectHashMap();
//    }
//    containers = new TLongObjectHashMap<GBTS>();
  }
  
  @Override
  public List<TimeSeries> timeSeries() {
    return final_list;
  }
  
  interface Foo extends TimeSeries {
    public void decode(final ArrayList<KeyValue> row,
        final RollupInterval interval);
    
    public void setGBAgg(final NumericArrayAggregator agg);
    
//    public ChronoUnit dedupe(final TSDB tsdb,
//        final boolean keep_earliest, 
//        final boolean reverse);
  }
  
  class PrevNextRate {
    long ts;
    double previous = Double.NaN;
    double next = Double.NaN;
  }
  
  class NumericA implements Foo {
    
    NumericArrayAggregator array_aggregator;
    private PooledObject nonOptimizedPooled;
    private double[] nonOptimizedArray;
    private int nonOptimizedIndex;
    private MutableNumericValue nonOptimizedDp;
    
    NumericA() {
      if (aggregator == Agg.NON_OPTIMIZED) {
        ObjectPool pool = node.pipelineContext().tsdb().getRegistry()
            .getObjectPool(DoubleArrayPool.TYPE);
        if (pool != null) {
          nonOptimizedPooled = ((ArrayObjectPool) pool).claim(64);
          nonOptimizedArray = (double[]) nonOptimizedPooled.object();
        } else {
          nonOptimizedArray = new double[64];
        }
        nonOptimizedDp = new MutableNumericValue();
      }
    }
    
    @Override
    public void setGBAgg(final NumericArrayAggregator agg) {
      array_aggregator = agg;
    }
    
    @Override
    public TimeSeriesId id() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (type == NumericArrayType.TYPE) {
        return Optional.of(new It());
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = Lists.newArrayList();
      its.add(new It());
      return null;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericArrayType.SINGLE_LIST;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }

    public void decode(ArrayList<KeyValue> row, RollupInterval interval) {
      final int base_timestamp = (int) schema.baseTimestamp(row.get(0).key());
      for (final KeyValue kv : row) {
        if (interval == null && (kv.qualifier().length & 1) == 0) {
          if (!((Tsdb1xHBaseQueryNode) node).fetchDataType(NUMERIC_TYPE)) {
            // filter doesn't want #'s
            // TODO - dropped counters
            continue;
          }
          addColumn(NUMERIC_PREFIX, base_timestamp, kv.qualifier(), kv.value());
        } else if (interval == null) {
          final byte prefix = kv.qualifier()[0];
          if (prefix == Schema.APPENDS_PREFIX) {
            if (!((Tsdb1xHBaseQueryNode) node).fetchDataType((byte) 1)) {
              // filter doesn't want #'s
              continue;
            } else {
              addColumn(Schema.APPENDS_PREFIX, base_timestamp, kv.qualifier(), kv.value());
            }
          }
        } else {
          //LOG.info("&&&&&& WTF? Bad prefix??: " + Bytes.pretty(kv.qualifier()));
        }
      }
    }
    
    public void addColumn(final byte prefix,
        final int base_timestamp,
        final byte[] qualifier, 
        final byte[] value) {
      final Accumulator accumulator = threadLocalAccs.get();
      double val;
      if (prefix == Schema.APPENDS_PREFIX) {
//        if (data == null) {
//          // sweet, copy
//          data = Arrays.copyOf(value, value.length);
//        } else {
//          final byte[] copy = new byte[data.length + value.length];
//          System.arraycopy(data, 0, copy, 0, data.length);
//          System.arraycopy(value, 0, copy, data.length, value.length);
//          data = copy;
//        }
        int idx = 0;
        //int qual_idx = 0;
        //int value_idx = 0;
        long offset;
        byte flag;
        int vlen;
        while (idx < value.length) {
          if ((value[idx] & NumericCodec.NS_BYTE_FLAG) == 
              NumericCodec.NS_BYTE_FLAG) {
            // TODO - !!!!
            continue;
//            timestamp = (base_time * 1000 * 1000 * 1000) + 
//                NumericCodec.offsetFromNanoQualifier(qualifier, qual_idx);
//            flag = NumericCodec.getFlags(qualifier, qual_idx, (byte) NumericCodec.NS_Q_WIDTH);
//            vlen = NumericCodec.getValueLengthFromQualifier(qualifier, 
//                qual_idx + NumericCodec.NS_Q_WIDTH - 1);
//            if ((flag & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
//              val = NumericCodec.extractFloatingPointValue(value, value_idx, flag);
//            } else {
//              val = NumericCodec.extractIntegerValue(value, value_idx, flag);
//            }
//            qual_idx += NumericCodec.NS_Q_WIDTH;
//            value_idx += vlen;
//            
//            long cmp = timestamp / 1000 / 1000 / 1000;
//            if (cmp < startTime) {
//              continue;
//            }
//            if (cmp >= endTime) {
//              return;
//            }
          } else if ((value[idx] & NumericCodec.MS_BYTE_FLAG) == 
              NumericCodec.MS_BYTE_FLAG) {
            // TODO -!!!!!
//            timestamp = (base_time * 1000 * 1000) + 
//                NumericCodec.offsetFromMsQualifier(qualifier, qual_idx);
//            vlen = NumericCodec.getValueLengthFromQualifier(qualifier, 
//                qual_idx + NumericCodec.MS_Q_WIDTH - 1);
//            if ((flag & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
//              val = NumericCodec.extractFloatingPointValue(value, value_idx, flag);
//            } else {
//              val = NumericCodec.extractIntegerValue(value, value_idx, flag);
//            }
//            qual_idx += NumericCodec.MS_Q_WIDTH;
//            value_idx += vlen;
//            
//            long cmp = timestamp / 1000 / 1000;
//            if (cmp < startTime) {
//              continue;
//            }
//            if (cmp >= endTime) {
//              return;
//            }
            continue;
          } else {
            offset = NumericCodec.offsetFromSecondQualifier(value, idx) / 1000L / 1000L / 1000L;
            flag = NumericCodec.getFlags(value, idx, (byte) NumericCodec.S_Q_WIDTH);
            vlen = NumericCodec.getValueLength(flag);
            idx += NumericCodec.S_Q_WIDTH;
            if ((flag & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
              val = NumericCodec.extractFloatingPointValue(value, idx, flag);
            } else {
              val = NumericCodec.extractIntegerValue(value, idx, flag);
            }
            idx += vlen;
            
            //LOG.info("______ RAW: " + (base_time + offset) + "  V: " + val);
            if (base_timestamp + offset < startTime) {
              //LOG.info("##### LT " + (base_timestamp + offset));
              continue;
            }
            if (base_timestamp + offset >= endTime) {
              //LOG.info("##### GT " + (base_timestamp + offset));
              return;
            }
          }
          
          // store it
          accumulator.add(offset, val);
        }
      } else {
        // two options:
        // 1) It's a raw put data point in seconds or ms (now nanos)
        // 2) It's an old-school compacted column either hetero or homogenous
        // regarding seconds or ms.
        if (qualifier.length == NumericCodec.S_Q_WIDTH) {
          final long offset = NumericCodec.offsetFromSecondQualifier(
              qualifier, 0) /  1000L / 1000L / 1000L;
          // handle older versions of OpenTSDB 1.x where there were some 
          // encoding issues that only affected second values.
          int vlen = NumericCodec.getValueLengthFromQualifier(qualifier, 
              qualifier.length - 1);
          if (value.length != vlen) {
            // TODO - log it in a counter somewhere
            if ((qualifier[qualifier.length - 1] & NumericCodec.FLAG_FLOAT) == 
                NumericCodec.FLAG_FLOAT) {
              byte flags = NumericCodec.getFlags(
                  qualifier, 0, (byte) NumericCodec.S_Q_WIDTH);
              val = NumericCodec.extractFloatingPointValue(
                  NumericCodec.fixFloatingPointValue(flags, value),
                  0, 
                  flags);
            } else {
              if (value.length == 8) {
                val = NumericCodec.extractIntegerValue(value, 0, (byte) 7);
              } else if (value.length == 4) {
                val = NumericCodec.extractIntegerValue(value, 0, (byte) 3);
              } else if (value.length == 2) {
                val = NumericCodec.extractIntegerValue(value, 0, (byte) 1);
              } else {
                val = NumericCodec.extractIntegerValue(value, 0, (byte) 0);
              }
            }
          } else {
            byte flags = NumericCodec.getFlags(
                qualifier, 0, (byte) NumericCodec.S_Q_WIDTH);
            if ((flags & NumericCodec.FLAG_FLOAT) == NumericCodec.FLAG_FLOAT) {
              val = NumericCodec.extractFloatingPointValue(qualifier, 0, flags);
            } else {
              val = NumericCodec.extractIntegerValue(qualifier, 0, flags);
            }
          }
          
          if (base_timestamp + offset < startTime) {
            //LOG.info("##### LT " + (base_timestamp + offset));
            return;
          } else if (base_timestamp + offset >= endTime) {
            //LOG.info("##### GT " + (base_timestamp + offset));
            return;
          }
          
          accumulator.add(offset, val);
        } else {
          //LOG.info("******* WTF?");
          // TODO! Drop it for now
//          // instead of branching more to see if it's an ms or ns column,
//          // we can just start iterating. Note that if the column is compacted
//          // and has a mixed time type sentinel at the end we'll allocate an
//          // extra value byte but we should never iterate or read it.
//          int write_idx = 0;
//          if (data == null) {
//            data = new byte[qualifier.length + value.length];
//          } else {
//            final byte[] copy = new byte[data.length + 
//                                         qualifier.length + 
//                                         value.length];
//            System.arraycopy(data, 0, copy, 0, data.length);
//            write_idx = data.length;
//            data = copy;
//          }
//          int qidx = 0;
//          int vidx = 0;
//          int vlen = 0;
//          while (qidx < qualifier.length) {
//            if ((qualifier[qidx] & NumericCodec.NS_BYTE_FLAG) == 
//                NumericCodec.NS_BYTE_FLAG) {
//              System.arraycopy(qualifier, qidx, data, write_idx, 
//                  NumericCodec.NS_Q_WIDTH);
//              write_idx += NumericCodec.NS_Q_WIDTH;
//              qidx += NumericCodec.NS_Q_WIDTH;
//            } else if ((qualifier[qidx] & NumericCodec.MS_BYTE_FLAG) == 
//                NumericCodec.MS_BYTE_FLAG) {
//              System.arraycopy(qualifier, qidx, data, write_idx, 
//                  NumericCodec.MS_Q_WIDTH);
//              write_idx += NumericCodec.MS_Q_WIDTH;
//              qidx += NumericCodec.MS_Q_WIDTH;
//            } else {
//              System.arraycopy(qualifier, qidx, data, write_idx, 
//                  NumericCodec.S_Q_WIDTH);
//              write_idx += NumericCodec.S_Q_WIDTH;
//              qidx += NumericCodec.S_Q_WIDTH;
//            }
//            vlen = NumericCodec.getValueLengthFromQualifier(qualifier, qidx - 1);
//            System.arraycopy(value, vidx, data, write_idx, vlen);
//            write_idx += vlen;
//            vidx += vlen;
//          }
//          
//          if (write_idx < data.length) {
//            // truncate in case there was a compacted column with the last
//            // byte set to 0 or 1.
//            data = Arrays.copyOfRange(data, 0, write_idx);
//          }
        }
      }
      //LOG.info("****************** FINISHED parsing SEGMENT: " + base_timestamp);
    }
    
    void flush(int base_timestamp) {
      final Accumulator accumulator = threadLocalAccs.get();
      final double[] aggs = threadLocalAggs.get();
      
      // TODO - close if we were missing a row
      int intervalOffset = 0;
      boolean intervalHasValue = false;
      boolean intervalInfectedByNans = false;
      
      // UGgg, may as well keep from locking and unlocking...
      //synchronized (array_aggregator) {
      for (int i = 0; i < accumulator.values.length; i++) {
        double v = accumulator.values[i];

        if (Double.isNaN(v)) {
          if (infectiousNans) {
            if (!intervalInfectedByNans) {
              intervalHasValue = false;
              intervalInfectedByNans = true;
            }
          }
        } else {
          if (!infectiousNans || !intervalInfectedByNans) {
            aggs[0] += v; // sum
            aggs[1]++; // count
            if (v < aggs[2]) {
              aggs[2] = v; // min
            }
            if (v > aggs[3]) {
              aggs[3] = v; // max
            }
            aggs[4] = v; // last
            if (!intervalHasValue) {
              intervalHasValue = true;
            }
          }
          
          if (aggregator == Agg.NON_OPTIMIZED) {
            if (nonOptimizedIndex + 1 >= nonOptimizedArray.length) {
              double[] temp = new double[nonOptimizedArray.length * 2];
              System.arraycopy(nonOptimizedArray, 0, temp, 0, nonOptimizedIndex);
              nonOptimizedArray = temp;
              if (nonOptimizedPooled != null) {
                nonOptimizedPooled.release();
              }
            }
            nonOptimizedArray[nonOptimizedIndex++] = v;
          }
        }
        intervalOffset++;

        if (intervalOffset == interval) { // push it to group by
          // TODO won't work for cross segment ds
          int intervalIndex = (int) (((base_timestamp + i) - startTime) / downsampleConfig.interval().get(ChronoUnit.SECONDS));
          if (intervalIndex < 0) {
//            LOG.info("*((((((( WTF? Index: " + intervalIndex + " for " + (base_timestamp + i) 
//                + " start: " + startTime + " at " + i);
            // reset interval
            for (int x = 0; x < aggs.length; x++) {
              aggs[x] = IDENTITY[x];
            }
            intervalOffset = 0;
            intervalHasValue = false;
            intervalInfectedByNans = false;
            continue;
          }
          if (intervalIndex >= downsampleConfig.intervals()) {
            //LOG.info("#### Beyond intervals: " + intervalIndex);
            break;
          }
//          LOG.info("********* INTERVALOFFSET: " + intervalOffset + "  Interval: " 
//              + interval + "  IDX: " + intervalIndex + "  i: " + i + "  TS: " + ((base_timestamp + i) - startTime));
          if (intervalHasValue) {
            switch (aggregator) {
              case SUM:
                if (reporting_average) {
                  v = aggs[0] / downsampleConfig.dpsInInterval();
                } else {
                  v = aggs[0];
                }
                break;
              case COUNT:
                v = aggs[1];
                break;
              case MIN:
                v = aggs[2];
                break;
              case MAX:
                v = aggs[3];
                break;
              case LAST:
                v = aggs[4];
                break;
              case AVG:
                v = aggs[0] / aggs[1];
                break;
              case NON_OPTIMIZED:
                nonOptimizedAggregator.run(nonOptimizedArray, 0, nonOptimizedIndex, downsampleConfig.getInfectiousNan(), nonOptimizedDp);
                nonOptimizedIndex = 0;
                v = nonOptimizedDp.toDouble();
                break;
              default:
                throw new UnsupportedOperationException(
                    "Unsupported aggregator: " + aggregator);
            }
//            if(logger.isTraceEnabled()) {
//              logger.trace("Add to group by interval index: {}  value: {}", intervalIndex, v);
//            }
            if (array_aggregator != null) {
              //LOG.info("                [" + Thread.currentThread().getName() + "]   ACCUMULATE: " + intervalIndex);
              array_aggregator.accumulate(v, intervalIndex);
            } else {
              throw new IllegalStateException("GB Agg can't be null!");
              //nonGroupByResults[intervalIndex++] = v;
            }
          }

          // reset interval
          for (int x = 0; x < aggs.length; x++) {
            aggs[x] = IDENTITY[x];
          }
          intervalOffset = 0;
          intervalHasValue = false;
          intervalInfectedByNans = false;
        }
      }
      //}
      accumulator.reset();
     //LOG.info("@@@@@@@@ Finished flush");
      //return ChronoUnit.SECONDS;
    }
  
    class It implements TypedTimeSeriesIterator<NumericArrayType>, TimeSeriesValue<NumericArrayType>, NumericArrayType {
      boolean has_next = true;
      @Override
      public boolean hasNext() {
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericArrayType> next() {
        has_next = false;
        return this;
      }

      @Override
      public void close() throws IOException {
        // TODO Auto-generated method stub
        
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
        return downsampleConfig.intervals();
      }

      @Override
      public boolean isInteger() {
        return false;
      }

      @Override
      public long[] longArray() {
        return null;
      }

      @Override
      public double[] doubleArray() {
        return array_aggregator.doubleArray();
      }

      @Override
      public TypeToken<NumericArrayType> type() {
        return NumericArrayType.TYPE;
      }

      @Override
      public TimeStamp timestamp() {
        return new SecondTimeStamp(startTime);
      }

      @Override
      public NumericArrayType value() {
        return this;
      }
      
    }
  }
    
  class State {
    NumericA last;
    long last_hash;
    int last_ts;
    TLongObjectMap<Foo> foos;
    TLongObjectMap<GBTS> containers;
    
    State() {
      foos = new TLongObjectHashMap();
      containers = new TLongObjectHashMap<GBTS>();
    }
    
    public void decode(final ArrayList<KeyValue> row,
        final RollupInterval interval) {
      int base_ts = (int) schema.baseTimestamp(row.get(0).key());
      final long hash = schema.getTSUIDHash(row.get(0).key());
      if (last_hash == hash) {
        if (base_ts != last_ts) {
          last.flush(last_ts);
        }
        last_ts = base_ts;
        last.decode(row, interval);
        return;
      } else if (last != null) {
        last.flush(last_ts);
      }
      
      last_ts = base_ts;
      last_hash = hash;
      //int bucket = Math.abs((int) hash % 16);
      //synchronized (buckets[bucket]) {
        last = (NumericA) foos.get(hash);
        if (last == null) {
          last = new NumericA();
          foos.put(hash, last);
          
//          StringBuilder buf = new StringBuilder()
//              .append("[");
//          for (int i = 0; i < gbConfig.getEncodedTagKeys().size(); i++) {
//            if (i > 0) {
//              buf.append(", ");
//            }
//            buf.append(Arrays.toString(gbConfig.getEncodedTagKeys().get(i)));
//          }
//          buf.append("]");
          //LOG.info("***************** GB TAGS: " + buf.toString());
          long group_hash = schema.groupByHashFromTSUID(row.get(0).key(), gbConfig.getEncodedTagKeys());
          GBTS group = containers.get(group_hash);
          if (group == null) {
            group = new GBTS();
            containers.put(group_hash, group);
          }
          
          group.add(last, new TSUID(schema.getTSUID(row.get(0).key()), schema));
//        }
      }
      
      last.decode(row, interval);
    }
  }
  
  class GBTS implements TimeSeries {
    NumericArrayAggregator array_aggregator;
    MergedTimeSeriesId.Builder id_builder = null;
    TimeSeriesId id = null;
    
    GBTS() {
      array_aggregator = (NumericArrayAggregator) factory.newAggregator(
          aggregatorConfig);
      id_builder = MergedTimeSeriesId.newBuilder();
    }
    
    void add(Foo foo, final TimeSeriesId id) { 
      foo.setGBAgg(array_aggregator);
      id_builder.addSeries(id);
    }
    
    void merge(GBTS other) {
      if (other.array_aggregator == null || array_aggregator == null) {
        LOG.error("WTF? Null aggs?");
        return;
      }
      
      array_aggregator.combine(other.array_aggregator);
    }
    
    @Override
    public TimeSeriesId id() {
      if (id == null) {
        id = id_builder.build();
      }
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterator(
        TypeToken<? extends TimeSeriesDataType> type) {
      if (type == NumericArrayType.TYPE) {
        return Optional.of(new It());
      }
      return Optional.empty();
    }

    @Override
    public Collection<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> iterators() {
      List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> its = Lists.newArrayList();
      its.add(new It());
      //LOG.info("*********** GETTING ITERATOR!");
      return its;
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return NumericArrayType.SINGLE_LIST;
    }

    @Override
    public void close() {
      try {
        array_aggregator.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    class It implements TypedTimeSeriesIterator<NumericArrayType>, TimeSeriesValue<NumericArrayType> {
      boolean has_next = array_aggregator.end() > array_aggregator.offset();
      
      It() {
        //LOG.info("***** NEXT: " + array_aggregator.offset() + " => " + array_aggregator.end());
      }
      
      @Override
      public boolean hasNext() {
        //LOG.info("******* HAS NEXT! " + has_next);
        return has_next;
      }

      @Override
      public TimeSeriesValue<NumericArrayType> next() {
        has_next = false;
        return this;
      }

      @Override
      public void close() throws IOException {
        // TODO Auto-generated method stub
        
      }

      @Override
      public TypeToken<NumericArrayType> getType() {
        return NumericArrayType.TYPE;
      }

      @Override
      public TimeStamp timestamp() {
        // TODO - what do I do?
        TimeStamp ts = new SecondTimeStamp(startTime);
        System.out.println(" ******* GOT TS sending upstream: " + ts.epoch());
        return ts;
      }

      @Override
      public TypeToken type() {
        return NumericArrayType.TYPE;
      }

      @Override
      public NumericArrayType value() {
        System.out.println("***** RETURNING " + array_aggregator);
        return array_aggregator;
      }
      
    }
  }
  
  /**
   * Parses a row for results. Since numerics are the most prevalent we
   * have a dedicated rowSeq for those (if we're told to fetch em). For 
   * other types we'll build out a map. After the row is finished we 
   * call {@link RowSeq#dedupe(boolean, boolean)} on each one then
   * pass it to the seq handler.
   * Note: Since it's the fast path we don't check for nulls/empty in
   * the row.
   * 
   * @param row A non-null and non-empty list of columns.
   * @param interval An optional interval, may be null.
   */
  public void decode(final ArrayList<KeyValue> row,
                     final RollupInterval interval) {
    if (interval != null && rollup_interval == null) {
      LOG.info("********** RESETTING ROLLUP INTERVAL");
      rollup_interval = interval;
      storage_interval = rollup_interval.getIntervals() * rollup_interval.getIntervalSeconds();
      threadLocalAccs = ThreadLocal.withInitial(() -> new Accumulator(rollup_interval.getIntervals()));
    }
    
    state.get().decode(row, interval);
  }
  
  public void decode2(final ArrayList<ArrayList<KeyValue>> rows,
                      final RollupInterval interval) {
    if (interval != null && rollup_interval == null) {
      LOG.info("********** RESETTING ROLLUP INTERVAL");
      rollup_interval = interval;
      storage_interval = rollup_interval.getIntervals() * rollup_interval.getIntervalSeconds();
      threadLocalAccs = ThreadLocal.withInitial(() -> new Accumulator(rollup_interval.getIntervals()));
    }
    
    State st = state.get();
    for (int i = 0; i < rows.size(); i++) {
      ArrayList<KeyValue> row = rows.get(i);
      if (row == null || row.isEmpty()) {
        // happens when filtering.
        continue;
      }
      st.decode(rows.get(i), interval);
    }
  }
  
//  public void finishThread() {
//    State s = state.get();
//    if (s.last != null) {
//      s.last.flush(s.last_ts);
//    }
//  }
  
  public void finalize() {
    TLongObjectMap<GBTS> containers = new TLongObjectHashMap<GBTS>();
    for (final State s : states) {
      if (s.last != null) {
        s.last.flush(s.last_ts);
      }
      
      // mergeroo!
      s.foos = null;
      TLongObjectIterator<GBTS> iterator = s.containers.iterator();
      while(iterator.hasNext()) {
        iterator.advance();
        GBTS extant = containers.get(iterator.key());
        if (extant == null) {
          containers.put(iterator.key(), iterator.value());
        } else {
          extant.merge(iterator.value());
          iterator.value().close();
        }
      }
      s.containers = null;
    }
    
    
    final_list = Lists.newArrayListWithExpectedSize(containers.size());
    TLongObjectIterator<GBTS> iterator = containers.iterator();
    while (iterator.hasNext()) {
      iterator.advance();
      final_list.add(iterator.value());
    }
    final_results = null;
    LOG.info("*********** DONE!!! " + final_list.size());
  }

  @Override
  public TimeSpecification timeSpecification() {
    return this;
  }
  
  class Accumulator {
    private double[] values;
    private int size;
    private boolean hasValue = false;

    Accumulator(int size) {
      this.values = new double[size];
      this.size = size;
      reset();
    }

    void add(long index, double value) {
      //LOG.info("----------- [" + Thread.currentThread().getName() + "] Add " + value + " at " + index);
      if (!hasValue) {
        hasValue = true;
      }
      
      if (Double.isNaN((values[(int) index])) || !keep_earliest) {
        values[(int) index] = value;
      }
    }

    void reset() {
      Arrays.fill(values, Double.NaN);
      hasValue = false;
    }

  }

  @Override
  public TimeStamp start() {
    return downsampleConfig.startTime();
  }

  @Override
  public TimeStamp end() {
    return downsampleConfig.endTime();
  }

  @Override
  public TemporalAmount interval() {
    return downsampleConfig.interval();
  }

  @Override
  public String stringInterval() {
    return downsampleConfig.getInterval();
  }

  @Override
  public ChronoUnit units() {
    return downsampleConfig.units();
  }

  @Override
  public ZoneId timezone() {
    return downsampleConfig.timezone();
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }
}

package net.opentsdb.query.egads.olympicscoring;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.DefaultRegistry;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.core.MockTSDB;
import net.opentsdb.core.PluginConfigValidator;
import net.opentsdb.core.PluginsConfig;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.core.PluginsConfig.PluginConfig;
import net.opentsdb.data.BaseTimeSeriesDatumStringId;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesDatum;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.data.types.alert.AlertType;
import net.opentsdb.data.types.numeric.MutableNumericValue;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultTimeSeriesDataSourceConfig;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QuerySinkCallback;
import net.opentsdb.query.SemanticQuery;
import net.opentsdb.query.SemanticQueryContext;
import net.opentsdb.query.anomaly.AnomalyConfig.ExecutionMode;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.storage.MockDataStoreFactory;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;

public class TestOlympicScoringNode {

  private static final int BASE_TIME = 1546300800;
  private static final String HOULRY_METRIC = "egads.metric.hourly";
  private static final String TAGK_STRING = "host";
  private static final String TAGV_A_STRING = "web01";
  private static final String TAGV_B_STRING = "web02";
  private static NumericInterpolatorConfig INTERPOLATOR;
  private static MockTSDB TSDB;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TSDB = new MockTSDB(true);
    TSDB.registry = new DefaultRegistry(TSDB);
    ((DefaultRegistry) TSDB.registry).initialize(true);
    
    if (!TSDB.getConfig().hasProperty("MockDataStore.register.writer")) {
      TSDB.config.register("MockDataStore.register.writer", true, false, "UT");
    }
    if (!TSDB.getConfig().hasProperty("MockDataStore.threadpool.enable")) {
      TSDB.config.register("MockDataStore.threadpool.enable", false, false, "UT");
    }
    
    MockDataStoreFactory factory = new MockDataStoreFactory();
    factory.initialize(TSDB, null).join(30000);
    ((DefaultRegistry) TSDB.registry).registerPlugin(
        TimeSeriesDataSourceFactory.class, null, (TSDBPlugin) factory);
    storeHourlyData();
    
    INTERPOLATOR = (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
        .setFillPolicy(FillPolicy.NOT_A_NUMBER)
        .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
        .setDataType(NumericType.TYPE.toString())
        .build();
  }
  
  @Test
  public void foo() throws Exception {
    
    SemanticQuery baseline_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + (3600 * 11)))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 12)))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .build();
    
    SemanticQuery egads_query = SemanticQuery.newBuilder()
        .setStart(Integer.toString(BASE_TIME + (3600 * 11)))
        .setEnd(Integer.toString(BASE_TIME + (3600 * 12)))
        .setMode(QueryMode.SINGLE)
        .addExecutionGraphNode(DefaultTimeSeriesDataSourceConfig.newBuilder()
            .setMetric(MetricLiteralFilter.newBuilder()
                .setMetric(HOULRY_METRIC)
                .build())
            .setId("m1")
            .build())
        .addExecutionGraphNode(DownsampleConfig.newBuilder()
            .setInterval("1m")
            .setAggregator("avg")
            .setFill(true)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("m1")
            .setId("ds")
            .build())
        .addExecutionGraphNode(OlympicScoringConfig.newBuilder()
            .setBaselinePeriod("1h")
            .setBaselineNumPeriods(3)
            .setBaselineAggregator("avg")
            .setBaselineQuery(baseline_query)
            .setSerializeObserved(true)
            .setSerializeThresholds(true)
            .setLowerThreshold(100)
            //.setUpperThreshold(100)
            .setMode(ExecutionMode.CONFIG)
            .addInterpolatorConfig(INTERPOLATOR)
            .addSource("ds")
            .setId("egads")
            .build())
        .build();
    
    Object waity = new Object();
    class Sink implements QuerySink {

      @Override
      public void onComplete() {
        // TODO Auto-generated method stub
        System.out.println("DONE!!");
        synchronized (waity) {
          waity.notify();
        }
      }

      @Override
      public void onNext(QueryResult next) {
        // TODO Auto-generated method stub
        System.out.println("WOOT! NEXT: " + next.dataSource());
        try {
          if (next.timeSpecification() != null) {
            System.out.println("     TIME SPEC: " + next.timeSpecification().start().epoch() + " " 
                + next.timeSpecification().end().epoch());
          }
          
          for (final TimeSeries ts : next.timeSeries()) {
            System.out.println("[SERIES] " + ts.id() + "  HASH: [" + ts.id().buildHashCode() + "] TYPES: " + ts.types());
            for (final TypedTimeSeriesIterator<? extends TimeSeriesDataType> it : ts.iterators()) {
              System.out.println("      IT: " + it.getType());
              int x = 0;
              StringBuilder buf = null;
              while (it.hasNext()) {
                TimeSeriesValue<? extends TimeSeriesDataType> value = it.next();
                
                if (it.getType() == NumericArrayType.TYPE) {
                  TimeSeriesValue<NumericArrayType> v = (TimeSeriesValue<NumericArrayType>) value;
                  if (value.value() == null) {
                    System.out.println("WTF? Null value at: " + v.timestamp());
                    continue;
                  }
                  if (v.value().isInteger()) {
                    System.out.println("   " + Arrays.toString(v.value().longArray()));
                  } else {
                    System.out.println("   " + Arrays.toString(v.value().doubleArray()));
                  }
                } else if (it.getType() == NumericType.TYPE) {
                  TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) value;
                  if (buf == null) {
                    buf = new StringBuilder()
                        .append("{");
                  }
                  if (x > 0) {
                    buf.append(", ");
                  }
                  buf.append(v.value().toDouble());
                  //System.out.println(v.timestamp().epoch() + "  " + v.value().toDouble());
                } else if (it.getType() == AlertType.TYPE) {
                  TimeSeriesValue<AlertType> v = (TimeSeriesValue<AlertType>) value;
                  System.out.println("   ALERT! " + v.timestamp().epoch() + "  " + v.value().message());
                }
                
                x++;
                if (x > 121) {
                  System.out.println("WHOOP? " + x);
                  return;
                }
              }
              
              if (buf != null) {
                buf.append("}");
                System.out.println("     " + buf.toString());
              }
              System.out.println("   READ: " + x);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          next.close();
        }
      }

      @Override
      public void onNext(PartialTimeSeries next, QuerySinkCallback callback) {
        // TODO Auto-generated method stub
        
      }

      @Override
      public void onError(Throwable t) {
        // TODO Auto-generated method stub
        t.printStackTrace();
        waity.notify();
      }
      
    }
    
    QueryContext ctx = SemanticQueryContext.newBuilder()
        .setTSDB(TSDB)
        .addSink(new Sink())
        .setQuery(egads_query)
        //.setQuery(baseline_query)
        .setMode(QueryMode.SINGLE)
        .build();
    ctx.initialize(null).join();
    System.out.println("  INITIALIZED. now fetching next");
    ctx.fetchNext(null);
    
    synchronized (waity) {
      waity.wait(10000);
    }
    System.out.println("---- EXIT ----");
  }
  
  static void storeHourlyData() throws Exception {
    WritableTimeSeriesDataStoreFactory factory = TSDB.getRegistry().getDefaultPlugin(WritableTimeSeriesDataStoreFactory.class);
    WritableTimeSeriesDataStore store = factory.newStoreInstance(TSDB, null);
    
    TimeSeriesDatumStringId id_a = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_A_STRING)
        .build();
    TimeSeriesDatumStringId id_b = BaseTimeSeriesDatumStringId.newBuilder()
        .setMetric(HOULRY_METRIC)
        .addTags(TAGK_STRING, TAGV_B_STRING)
        .build();
    
    int ts = BASE_TIME;
    int wrote = 0;
    for (int x = 0; x < 12; x++) {
      for (int i = 0; i < 60; i++) {
        double value = Math.sin((ts % 3600) / 10);
        
        MutableNumericValue v = 
            new MutableNumericValue(new SecondTimeStamp(ts), value);
        store.write(null, TimeSeriesDatum.wrap(id_a, v), null).join();
        
        if (ts == 1546340580) {
          v = new MutableNumericValue(new SecondTimeStamp(1546340610), value * 10);
          store.write(null, TimeSeriesDatum.wrap(id_a, v), null).join();
          wrote++;
        }
        
        v = new MutableNumericValue(new SecondTimeStamp(ts), value * 10);
        store.write(null, TimeSeriesDatum.wrap(id_b, v), null).join();
        ts += 60;
        wrote++;
      }
    }
    System.out.println(" ------ WROTE TO " + System.identityHashCode(store) + " STORE!  " + wrote);
  }
}

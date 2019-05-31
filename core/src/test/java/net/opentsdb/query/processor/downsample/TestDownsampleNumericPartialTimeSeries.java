package net.opentsdb.query.processor.downsample;

import static org.mockito.Mockito.mock;

import org.junit.Test;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.interpolation.types.numeric.NumericSummaryInterpolatorConfig;
import net.opentsdb.utils.DateTime;

public class TestDownsampleNumericPartialTimeSeries {

  @Test
  public void foo() throws Exception {
    DownsampleConfig config = (DownsampleConfig) DownsampleConfig.newBuilder()
        .setAggregator("sum")
        .setId("foo")
        .setInterval("15s")
        .setStart("1514843302")
        .setEnd("1514846902")
        .addInterpolatorConfig(mock(NumericInterpolatorConfig.class))
        .build();
    
    // old == new
    TimeStamp set_a_start = new SecondTimeStamp(1514840400);
    TimeStamp set_a_end = new SecondTimeStamp(1514844000);
    
    long interval = DateTime.parseDuration(config.getInterval());
    if (interval > (set_a_end.msEpoch() - set_a_start.msEpoch())) {
      System.out.println("BIGGER!!!");
    } else {
      long leftovers = (set_a_end.msEpoch() - set_a_start.msEpoch()) % interval;
      if (leftovers == 0) {
        // fits nicely
        
      } else {
        // no bueno, odd alignment
        
      }
      System.out.println(leftovers);
    }
  }
  
  
  @Test
  public void foo2() throws Exception {
    long ts = 1514844000000L;
    ts = ts - (ts % (3600000L * 24L));
    System.out.println(ts);
  }
}

package net.opentsdb.formatters;

import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.DataQuery;
import net.opentsdb.tsd.HttpQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ascii extends TSDFormatter{
//overload the log factory
  private static final Logger LOG = LoggerFactory.getLogger(Ascii.class);
 
  public Ascii(final TSDB tsdb){
    super(tsdb);
  }
  
  public String getEndpoint(){
    return "ascii";
  }
  
  public String getOutput(){
    String ascii = "";
    
    try{
      final StringBuilder tagbuf = new StringBuilder();
      
//      for (final TimeSeries ts : this.timeseries) {
//        tagbuf.setLength(0);
//        for (final Map.Entry<String, String> tag : ts.tags.entrySet()) {
//          tagbuf.append(' ').append(tag.getKey())
//            .append('=').append(tag.getValue());
//        }
//        for (Entry<Long, Object> entry : ts.dps.entrySet()) {
//          ascii += ts.metric_name + " " + entry.getKey() + " ";
//          if (TimeSeries.isInteger(entry.getValue())) {
//            ascii += ((Long)entry.getValue()).toString();
//          } else {
//            final double value = (Double)entry.getValue();
//            if (value != value || Double.isInfinite(value)) {
//              throw new IllegalStateException("NaN or Infinity:" + value
//                + " d=" + entry.getValue());
//            }
//            ascii += value;
//          }
//          ascii += tagbuf + "\n";
//        }
//      }
      return ascii;
    } catch(IllegalStateException e){
      String error = e.getMessage();
      LOG.error(error);
      return error;
    }
  }
  
  public boolean validateQuery(final DataQuery dataquery){
    return true;
  }
}

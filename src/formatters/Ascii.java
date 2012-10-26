package net.opentsdb.formatters;

import java.util.Map;

import net.opentsdb.core.DataPoints;
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
  
  /**
   * Returns rows of complete data points in the format:
   * <metric> <timestamp> <value> <tag pairs>
   * @param query The HTTPQuery to parse
   * @return Returns true
   */
  public boolean handleHTTPGet(final HttpQuery query){  
    StringBuilder output = new StringBuilder(this.datapoints.size() * 1024);

    for (DataPoints dp : this.datapoints){
      
      // build the tags once for speed
      StringBuilder tags = new StringBuilder(dp.getTags().size() * 256);
      int counter=0;
      for (Map.Entry<String, String> pair : dp.getTags().entrySet()){
        if (counter > 0)
          tags.append(" ");
        tags.append(pair.getKey()).append("=").append(pair.getValue());
        counter++;
      }
      // now build the individual rows
      for(int i=0; i<dp.size(); i++){
        output.append(dp.metricName()).append(" ");
        output.append(dp.timestamp(i)).append(" ");
        output.append(dp.isInteger(i) ? dp.longValue(i) : dp.doubleValue(i)).append(" ");
        output.append(tags);
        output.append("\n");
      }
    }
    
    query.sendReply(output.toString());    
    return true;
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

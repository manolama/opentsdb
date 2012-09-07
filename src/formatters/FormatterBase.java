package net.opentsdb.formatters;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TimeSeries;

/**
 * Formatters for input and output must override this class
 *
 */
public class FormatterBase {
  private static final Logger LOG = LoggerFactory.getLogger(FormatterBase.class);
  
  protected ArrayList<TimeSeries> timeseries = null;
   
  final public ArrayList<TimeSeries> getTimeseries(){
    return this.timeseries;
  }
  
  final public void setTimeseries(final ArrayList<TimeSeries> ts){
    this.timeseries = ts;
  }
  
  public String getOutput(){
    LOG.warn("GetOutput has not been implemented");
    return "GetOutput has not been implemented";
  }
  
  public Boolean parseInput(final String data){
    LOG.warn("ParseInput has not been implemented");
    return false;
  }
}

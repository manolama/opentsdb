package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.HttpQuery;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base formatter class that must be overridden
 *
 */
public abstract class TSDFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(TSDFormatter.class);
  
  protected List<DataPoints> datapoints = new ArrayList<DataPoints>();
  final TSDB tsdb;
  
  public TSDFormatter(final TSDB tsdb){
    this.tsdb = tsdb;
  }
  
  public final void putDatapoints(final DataPoints dps){
    this.datapoints.add(dps);
  }
  
  /**
   * Method that will format the data points returned from a user's query and push
   * them back via HTTP
   * 
   * NOTE: This method must return the data to the user via the query object. 
   * It won't happen on it's own.
   * 
   * @param query The HTTP query to work with
   * @return True if the method completed successfully, false if there was an error
   */
  public boolean handleHTTPGet(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Not implemented");
    return false;
  }
  
  /**
   * Method that will parse the POST data from a query and store the results
   * 
   * NOTE: This method must return an answer to the user via the query object. 
   * It won't happen on it's own.
   * 
   * @param query The HTTP query to work with
   * @return True if the method completed successfully, false if there was an error
   */
  public boolean handleHTTPPut(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Not implemented");
    return false;
  }
  
  public Boolean handleTelnetGet(String[] command){
    LOG.warn("Method has not been implemented");
    return false;
  }
  
  public Boolean handleTelnetPut(String[] command){
    LOG.warn("Method has not been implemented");
    return false;
  }

  public static void collectStats(final StatsCollector collector){
    LOG.warn("Method has not been implemented");
  }
}

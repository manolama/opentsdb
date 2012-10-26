package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.DataQuery;
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
  private static final HashMap<String, Constructor> formatter_map = new HashMap<String, Constructor>();
  private static final ArrayList<String> formatters = new ArrayList<String>(){{
    add("net.opentsdb.formatters.Ascii");
    add("net.opentsdb.formatters.CollectdJSON");
    add("net.opentsdb.formatters.TsdbJSON");
    add("net.opentsdb.graph.GnuGraphFormatter");
  }
    private static final long serialVersionUID = 5304450759358216935L;
  };
  
  protected static final AtomicLong requests = new AtomicLong();
  protected static final AtomicLong hbase_errors = new AtomicLong();
  protected static final AtomicLong invalid_values = new AtomicLong();
  protected static final AtomicLong illegal_arguments = new AtomicLong();
  protected static final AtomicLong unknown_metrics = new AtomicLong();
  
  protected List<DataPoints> datapoints = new ArrayList<DataPoints>();
  protected final TSDB tsdb;
  protected DataQuery query;
  
  public TSDFormatter(final TSDB tsdb){
    this.tsdb = tsdb;
  }
  
  public abstract String getEndpoint();
  
  public final void putDatapoints(final DataPoints dps){
    this.datapoints.add(dps);
  }
  
  public abstract boolean validateQuery(final DataQuery query);
  
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

  public static boolean initClassMap(final TSDB tsdb){
    if (formatters == null || formatters.size() < 1){
      LOG.error("No formatters were listed in the names list");
      return false;
    }
    
    for (String formatter : formatters){
      try {
        Class f = Class.forName(formatter);
        
        Constructor[] ctors = f.getDeclaredConstructors();
        Constructor ctor = null;
        for (int i = 0; i < ctors.length; i++) {
            ctor = ctors[i];
            if (ctor.getGenericParameterTypes().length == 1)
              break;
        }
        
        ctor.setAccessible(true);
        
        TSDFormatter temp = (TSDFormatter)ctor.newInstance(tsdb);
        formatter_map.put(temp.getEndpoint(), ctor);
        
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        LOG.error(String.format("Unable to load class [%s]", formatter));
      } catch (IllegalArgumentException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InstantiationException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    if (formatter_map.size() > 0)
      return true;
    LOG.error("Unable to load any formatter classes");
    return false;
  }

  public static TSDFormatter getFormatter(final String endpoint, final TSDB tsdb){
    Constructor ctor = formatter_map.get(endpoint);
    if (ctor == null){
      LOG.warn(String.format("Unable to find formatter for endpoint [%s]", endpoint));
      return null;
    }
    
    try {
      return (TSDFormatter)ctor.newInstance(tsdb);
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public static Set<String> listFormatters(){
    return formatter_map.keySet();
  }
}

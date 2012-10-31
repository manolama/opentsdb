package net.opentsdb.formatters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.StatsCollector.StatsDP;
import net.opentsdb.tsd.DataQuery;
import net.opentsdb.tsd.HttpQuery;
import net.opentsdb.tsd.HttpQuery.HttpError;
import net.opentsdb.search.SearchQuery.SearchResults;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.mortbay.jetty.servlet.PathMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

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
  
  protected static final AtomicLong storage_errors = new AtomicLong();
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
  
  public String contentType(){
    return "text/plain";
  }
  
// HTTP HANDLERS --------------------------------------------------------------
  
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
  public boolean handleHTTPDataGet(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Method has not been implemented");
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
  public boolean handleHTTPDataPut(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPStats(final HttpQuery query, ArrayList<StatsDP> stats){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }

  public boolean handleHTTPMetaGet(final HttpQuery query, final Object meta){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPMetaPut(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPSearch(final HttpQuery query, SearchResults results){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPGroupby(final HttpQuery query, SearchResults results){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPSuggest(final HttpQuery query, final List<String> results){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPVersion(final HttpQuery query, final HashMap<String, Object> version){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPFormatters(final HttpQuery query, 
      final HashMap<String, ArrayList<String>> formatters){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  
  public boolean handleHTTPError(final HttpQuery query, final HttpError error){
    JSON codec = new JSON(error);
    query.sendReply(error.status, codec.getJsonString());
    return true;
  }
  
// TELNET HANDLERS ------------------------------------------------------------
  
  public boolean handleTelnetDataGet(String[] command, final Channel chan){
    LOG.warn("Method has not been implemented");
    return false;
  }
  
  public Deferred<Object> handleTelnetDataPut(String[] command, final Channel chan){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }

  public Deferred<Object> handleTelnetStats(String[] command, final Channel chan, 
      ArrayList<StatsDP> stats){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetMetaGet(String[] command, final Channel chan, 
      final Object meta){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetMetaPut(String[] command, final Channel chan){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetSearch(String[] command, final Channel chan, 
      SearchResults results){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetGroupby(String[] command, final Channel chan, 
      SearchResults results){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetSuggest(String[] command, final Channel chan, 
      final List<String> results){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetVersion(String[] command, final Channel chan, 
      final HashMap<String, Object> version){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
  
  public Deferred<Object> handleTelnetFormatters(String[] command, final Channel chan, 
      final HashMap<String, ArrayList<String>> formatters){
    LOG.warn("Method has not been implemented");
    return Deferred.fromResult(null);
  }
   
  public static void collectStats(final StatsCollector collector, final TSDB tsdb){
    collector.record("formatters.storage_errors", storage_errors.get());
    collector.record("formatters.invalid_values", invalid_values.get());
    collector.record("formatters.illegal_arguments", illegal_arguments.get());
    collector.record("formatters.unknown_metrics", unknown_metrics.get());
    
    // todo - do this for each formatter as well
    for (String formatter : formatters){
      try {
        Class f = Class.forName(formatter);
        Method[] methods = f.getMethods();
        for (Method m : methods){
          if (m.getName().compareTo("collectClassStats") == 0){
            //m.setAccessible(true);
            m.invoke(f, collector);
          }
        }
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        LOG.error(String.format("Unable to load class [%s]", formatter));
      } catch (IllegalArgumentException e) {
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
      //LOG.warn(String.format("Unable to find formatter for endpoint [%s]", endpoint));
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

  public static HashMap<String, ArrayList<String>> listFormatters(){
    HashMap<String, ArrayList<String>> fmts = new HashMap<String, ArrayList<String>>();
    try {
      for (String formatter : formatters){
        Class f = Class.forName(formatter);
        ArrayList<String> publics = new ArrayList<String>();
        Method[] methods = f.getDeclaredMethods();
        for (Method m : methods){
          if (Modifier.isPublic(m.getModifiers())){
            if (m.getName().indexOf("handle") == 0)
              publics.add(m.getName().substring(6));
          }
        }
        fmts.put(formatter.substring(formatter.lastIndexOf(".") + 1).toLowerCase(), publics);
      }
      return fmts;
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }   
    return null;
  }
}

package net.opentsdb.query.anomaly;

import net.opentsdb.query.QueryNodeConfig;

public interface AnomalyConfig extends QueryNodeConfig {

  public static enum ExecutionMode {
    /** Generates the prediction if necessary and evaluates, returning only the 
      * prediction, observed, alerts, upper and lower thresholds. */
    EVALUATE,
    
    /** Just generates the prediction and caches it if appropriate. Returns an
     * empty data set with the HTTP status code indicating success or failure. */
    PREDICT,
    
    /** Does not cache the predictions but is used in building a query in that
     * it will return all of the data including base lines. */
    CONFIG,
  }
  
  public ExecutionMode getMode();
  
  public boolean getSerializeObserved();
  
  public boolean getSerializeThresholds();
  
}

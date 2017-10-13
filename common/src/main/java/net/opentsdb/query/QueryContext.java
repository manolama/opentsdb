package net.opentsdb.query;

import net.opentsdb.stats.Span;
import net.opentsdb.stats.Tracer;

public interface QueryContext {

  /**
   * Returns the current listener for this component.
   * @return The listener if set, null if no listener has been set.
   */
  public QueryListener getListener();
  
  /**
   * Returns the mode the query is executing in.
   * @return The non-null query mode.
   */
  public QueryMode mode();
  
  /**
   * Travels downstream the pipeline to fetch the next set of results. 
   * @throws IllegalStateException if no listener was set on this context.
   */
  public void fetchNext();
  
  /**
   * Closes the pipeline and releases all resources.
   */
  public void close();
  
  public Span querySpan();
  
  public Tracer tracer();
}

package net.opentsdb.query.execution.gateway;

import net.opentsdb.query.QuerySink;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.stats.Span;

public interface QueryGateway {

  /**
   * TODO - return an object that references the query so it can
   * move around hosts, etc.
   * @param query
   * @param sink
   * @param span
   * @return
   */
  public Object runQuery(final TimeSeriesQuery query, 
                         final QuerySink sink, 
                         final Span span);
}

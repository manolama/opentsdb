package net.opentsdb.data;

import java.io.InputStream;
import java.io.OutputStream;

import net.opentsdb.query.QueryResult;
import net.opentsdb.query.execution.serdes.SerdesOptions;
import net.opentsdb.query.pojo.TimeSeriesQuery;

public interface TimeSeriesSerDes {
  /**
   * Writes the given data to the stream.
   * @param query A non-null query.
   * @param options Options for serialization.
   * @param stream A non-null stream to write to.
   * @param data A non-null query result set.
   */
  public void serialize(final TimeSeriesQuery query,
                        final SerdesOptions options,
                        final OutputStream stream, 
                        final QueryResult data);
  
  /**
   * Parses the given stream into the proper data object.
   * @param options Options for deserialization.
   * @param stream A non-null stream. May be empty.
   * @return A non-null query result object.
   */
  public QueryResult deserialize(final SerdesOptions options, final InputStream stream);
}

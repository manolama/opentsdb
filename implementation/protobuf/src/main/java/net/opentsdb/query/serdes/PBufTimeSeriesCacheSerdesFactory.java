package net.opentsdb.query.serdes;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;

import net.opentsdb.query.QueryResult;

/**
 * A simple factory for the protobuf serialization and caching functionality.
 *
 */
public class PBufTimeSeriesCacheSerdesFactory extends BaseTSDBPlugin 
  implements TimeSeriesCacheSerdesFactory {
    public static final String TYPE = "PBufTimeSeriesCacheSerdes";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Deferred<Object> initialize(final TSDB tsdb, final String id) {
        this.tsdb = tsdb;
        this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
        return Deferred.fromResult(null);
    }

    @Override
    public TimeSeriesCacheSerdes getSerdes() {
      return new PBufTimeSeriesCacheSerdes();
    }
}

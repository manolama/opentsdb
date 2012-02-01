package net.opentsdb.tsd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;

/**
 * Handles aggregation function information fetching
 */
final class AggregatorsRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatorsRPC.class);
  
  /**
   * Returns a list of aggregation functions supported by the TSD
   * in JSON format
   * @param tsdb Not used in this case
   * @param query HTTP query to respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && HttpCache.readCache(query_hash, query)){
      return;
    }
    
    final String jsonp = JsonHelper.getJsonPFunction(query);
    final JsonHelper response = new JsonHelper(Aggregators.set());
 
    // build our cache object, store and reply
    HttpCacheEntry entry = new HttpCacheEntry(query_hash, 
        jsonp.isEmpty() ? response.getJsonString().getBytes() 
            : response.getJsonPString(jsonp).getBytes(),
            "", /* don't bother persisting these */
            false,
            86400);
    if (!nocache && !HttpCache.storeCache(entry)){
      LOG.warn("Unable to cache emitter for key [" + query_hash + "]");
    }
    query.sendReply(entry.getData());
  }
}

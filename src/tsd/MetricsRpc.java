package net.opentsdb.tsd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;

/**
 * STUB
 *
 */
class MetricsRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsRpc.class);

  public void execute(final TSDB tsdb, final HttpQuery query) {
    final boolean nocache = query.hasQueryStringParam("nocache");
    final int query_hash = query.getQueryStringHash();
    if (!nocache && HttpCache.readCache(query_hash, query)){
      return;
    }
    final JsonHelper response;
    
    // TEMP STUB
    final String jsonp = JsonHelper.getJsonPFunction(query);
    if (query.hasQueryStringParam("tagk"))
      response = new JsonHelper(tsdb.getTagNames());
    else if (query.hasQueryStringParam("tagv"))
      response = new JsonHelper(tsdb.getTagValues());
    else
      response = new JsonHelper(tsdb.getMetrics());
    
    // build our cache object, store and reply
    HttpCacheEntry entry = new HttpCacheEntry(query_hash, 
        jsonp.isEmpty() ? response.getJsonString().getBytes() 
            : response.getJsonPString(jsonp).getBytes(),
            "", /* don't bother persisting these */
            false,
            30);
    if (!nocache && !HttpCache.storeCache(entry)){
      LOG.warn("Unable to cache [" + query_hash + "]");
    }
    query.sendReply(entry.getData());
  }
  
}

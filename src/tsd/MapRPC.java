package net.opentsdb.tsd;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueIdMap;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(MapRPC.class);

  public void execute(final TSDB tsdb, final HttpQuery query) {
    LOG.trace(query.request.getUri());

    Pattern p = Pattern.compile("^/.*\\/(\\w+).*$");
    Matcher m = p.matcher(query.request.getUri());
    String endpoint = "";
    if (m.find() && m.groupCount() >= 1)
      endpoint = m.group(1);

    // if they didn't choose an endpoint, dump the timeseries UIDs
    if (endpoint.length() < 1 ||
        endpoint.toLowerCase().compareTo("timeseries") == 0) {
      // return the tsd list
      Set<String> ts_uids = tsdb.ts_uids;
      if (ts_uids == null){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Timestamp UIDs have not been loaded yet");
        return;
      }
      JSON codec = new JSON(ts_uids);
      query.sendReply(codec.getJsonString());
      return;
    }
    
    String uid = query.getQueryStringParam("uid");
    String map_type = query.getQueryStringParam("map_type");

    if (uid == null || uid.length() < 1) {
      query.sendError(HttpResponseStatus.BAD_REQUEST,
          "Missing the [UID] parameter");
      return;
    }
    if (map_type == null || map_type.length() < 1) {
      query.sendError(HttpResponseStatus.BAD_REQUEST,
          "Missing the [map_type] parameter");
      return;
    }

    // try to fetch the map
    UniqueIdMap map = null;
    if (endpoint.toLowerCase().compareTo("metric") == 0)
      map = tsdb.metrics.getMap(uid);
    else if (endpoint.toLowerCase().compareTo("tagk") == 0)
      map = tsdb.tag_names.getMap(uid);
    else if (endpoint.toLowerCase().compareTo("tagv") == 0)
      map = tsdb.tag_values.getMap(uid);
    else {
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unsupported endpoint");
      return;
    }

    if (map == null) {
      query.sendError(HttpResponseStatus.BAD_REQUEST,
          "Unable to find matching UID object");
      return;
    }

    // todo(cl) determine if we should null out attempts to get a map of the same data
    // type as the endpoint. E.g. if we want metrics for a metric, it *should* only give us
    // 1 result, the UID requested. BUT since we're scanning the TS UID list, it will give us
    // all other metrics that share tag pairs with *this* metric. That *could* be useful.
    
    // determine what the user wants from the map and give it to them
    Set<String> uids = null;
    if (map_type.toLowerCase().compareTo("metric") == 0)
      uids = map.getMetrics(tsdb.ts_uids, (short) 3);
    else if (map_type.toLowerCase().compareTo("tagk") == 0)
      uids = map.getTags("tagk", (short) 3);
    else if (map_type.toLowerCase().compareTo("tagv") == 0)
      uids = map.getTags("tagv", (short) 3);
    else if (map_type.toLowerCase().compareTo("tags") == 0)
      uids = map.getTags();
    else if (map_type.toLowerCase().compareTo("timeseries") == 0)
      uids = map.getTSUIDs(tsdb.ts_uids, (short) 3);

    if (uids == null) {
      query.sendError(HttpResponseStatus.BAD_REQUEST,
          "Unable to find map matching requested type");
      return;
    }

    // serialize
    JSON codec = new JSON(uids);
    query.sendReply(codec.getJsonString());
    return;
  }
}

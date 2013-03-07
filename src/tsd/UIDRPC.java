package net.opentsdb.tsd;

import java.util.HashMap;
import java.util.HashSet;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UIDRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(UIDRPC.class);
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    
    HashSet<String> incoming = new HashSet<String>();
    String type = "";
    String content = query.getPostData();
    if (content.isEmpty()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing data");
      return;
    }
    
    if (query.hasQueryStringParam("type"))
      type = query.getQueryStringParam("type");
    
    JSON codec = new JSON(incoming);
    if (!codec.parseObject(content)){
      query.sendError(HttpResponseStatus.BAD_REQUEST, codec.getError());
      return;
    }
    incoming = (HashSet<String>)codec.getObject();
    
    HashMap<String, String> uids = new HashMap<String, String>();
    for (String val : incoming){
      if (type.compareTo("metric") == 0){
        uids.put(val, UniqueId.IDtoString(tsdb.metrics.getOrCreateId(val)));
      }else if (type.compareTo("tagk") == 0){
        uids.put(val, UniqueId.IDtoString(tsdb.tag_names.getOrCreateId(val)));
      }else if (type.compareTo("tagv") == 0){
        uids.put(val, UniqueId.IDtoString(tsdb.tag_values.getOrCreateId(val)));
      }else{
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing or invalid type");
        return;
      }
    }
    
    codec = new JSON(uids);
    query.sendReply(codec.getJsonBytes());
  }
}

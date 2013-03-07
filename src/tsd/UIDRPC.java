package net.opentsdb.tsd;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
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
    try {
      incoming = (HashSet<String>)JSON.parseToObject(content, HashSet.class);
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    
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
    
    try {
      query.sendReply(JSON.serializeToBytes(uids));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
  }
}

package net.opentsdb.tsd;

import java.io.File;
import java.io.IOException;

import net.opentsdb.core.TSDB;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

public class CacheRPC implements HttpRpc {

  
  /**
  * Handles various HTTP commands related to the /cache endpoint such as
  * returning cache metadata or flushing the cache Note: responses are not
  * cached, naturally
  * @param tsdb not really used in this case
  * @param query HTTP query to respond to
  */
  public void execute(final TSDB tsdb, final HttpQuery query) {
   // respond with metadata about each cache object in a JSON format
   final String file = query.hasQueryStringParam("file") ? query
       .getQueryStringParam("file") : "";
  
   if (!file.isEmpty()) {
     // get the cache directory
     String basepath = tsdb.getConfig().cacheDirectory();
     if (System.getProperty("os.name").contains("Windows")) {
       if (!basepath.endsWith("\\"))
         basepath += "\\";
     } else {
       if (!basepath.endsWith("/"))
         basepath += "/";
     }
  
     // see if the file exists
     if (new File(basepath + file).exists()) {
       // TODO lookup the cache entry to get the actual cache time
       try {
         query.sendFile(basepath + file, 30);
       } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
       }
     } else {
       query.sendReply(HttpResponseStatus.NOT_FOUND, "File [" + file
           + "] not found");
     }
   } else if (query.hasQueryStringParam("flush")) {
     // TODO flush the cache
     query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented");
   } else {
     // the default is to print info about the cache
//     final JSON_HTTP response = new JSON_HTTP(query.cache);
//     query.sendReply(jsonp.isEmpty() ? response.getJsonString() : response
//         .getJsonPString(jsonp));
   }
  }
}

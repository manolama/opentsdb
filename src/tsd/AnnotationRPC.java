package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotationRPC implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(AnnotationRPC.class);

  public void execute(final TSDB tsdb, final HttpQuery query) {   
    LOG.trace(query.request.getUri());
    // GET
    if (query.getMethod() == HttpMethod.GET) {
      Annotation note = this.getAnnotation(tsdb, query);
      if (note == null)
        return;
      List<Annotation> notes = new ArrayList<Annotation>();
      notes.add(note);
      query.getFormatter().handleHTTPAnnotation(query, notes);      
      // POST
    } else if (query.getMethod() == HttpMethod.POST ||
        query.getMethod() == HttpMethod.PUT ) {    
      
      final String content = query.getPostData();
      if (content == null || content.length() < 1){
        // we may have a delete, so check for that
        if (!query.hasQueryStringParam("delete")){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing POST data");
          return;
        }    
        
        // see if it exists first
        Annotation note = this.getAnnotation(tsdb, query);
        if (note == null)
          return;
        
        if (note.delete(tsdb)){
          // remove from index
          //tsdb.annotation_search_writer.deleteDoc("uid", note.getSearchUID());
          
          HashMap<String, String> response = new HashMap<String, String>();
          response.put("delete", "successful");
          response.put("tsuid", note.getTsuid());
          response.put("start_time", Long.toString(note.getStart_time()));
          try {
            query.sendReply(JSON.serializeToBytes(response));
          } catch (JsonParseException e) {
            query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
            return;
          } catch (IOException e) {
            query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
            return;
          }
        }else{
          query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error removing annotation from storage");
        }
        return;
      }
            
      try {
        Annotation note = (Annotation)JSON.parseToObject(content, Annotation.class);
        if (!note.store(tsdb)){
          query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving to storage");
          return;
        }
   
        List<Annotation> notes = new ArrayList<Annotation>();
        notes.add(note);
        query.getFormatter().handleHTTPAnnotation(query, notes);
        
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      }
    }
  }
  
  private Annotation getAnnotation(final TSDB tsdb, final HttpQuery query){
    if (!query.hasQueryStringParam("tsuid")){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tsuid parameter");
      return null;
    }
    if (!query.hasQueryStringParam("ts")){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing ts parameter");
      return null;
    }
    
    try{
      String tsuid = query.getQueryStringParam("tsuid");
      long ts = Long.parseLong(query.getQueryStringParam("ts"));
    
      Annotation note = Annotation.getFromStorage(tsdb, tsuid, ts);
      if (note == null){
        query.sendError(HttpResponseStatus.NOT_FOUND, "Annotation was not found");
        return null;
      }
      return note;
    } catch (NumberFormatException nfe){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse ts value");
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
    }
    return null;
  }
}

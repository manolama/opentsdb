package net.opentsdb.servlet.resources;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonGenerator;

import net.opentsdb.utils.JSON;

@Path("api/stream")
public class TestStream {

  @GET
  public void get(final @Context ServletConfig servlet_config, 
                      final @Context HttpServletRequest request,
                      final @Context HttpServletResponse response) throws Exception {
    AsyncContext ctx = request.startAsync();
//    final StreamingOutput stream = new StreamingOutput() {
//      @Override
//      public void write(OutputStream output) 
//          throws IOException, WebApplicationException {
        ChunkedOutputStream cos = new ChunkedOutputStream(response.getOutputStream());
        for (int i = 0; i < 4; i++) {
          final JsonGenerator json = JSON.getFactory().createGenerator(cos);
          
          json.writeStartObject();
          json.writeStringField("Hello", "FromChunker!");
          json.writeNumberField("Chunk", i);
          json.writeEndObject();
          
          json.flush();
          //json.close();
          
          System.out.println("FLUSHED!!");
          System.out.flush();
          
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        cos.done();
        cos.close();
        ctx.dispatch();
        System.out.println("All done!");
//      }
//    };
//    return Response.ok()
//        .header("Transfer-Encoding", "chunked")
//        .entity(stream)
//        .type(MediaType.APPLICATION_JSON)
//        .build();
  }
  
//  @GET
//  public Response get(final @Context ServletConfig servlet_config, 
//                      final @Context HttpServletRequest request) throws Exception {
//    final StreamingOutput stream = new StreamingOutput() {
//      @Override
//      public void write(OutputStream output) 
//          throws IOException, WebApplicationException {
//        ChunkedOutputStream cos = new ChunkedOutputStream(output);
//        for (int i = 0; i < 4; i++) {
//          final JsonGenerator json = JSON.getFactory().createGenerator(cos);
//          
//          json.writeStartObject();
//          json.writeStringField("Hello", "FromChunker!");
//          json.writeNumberField("Chunk", i);
//          json.writeEndObject();
//          
//          json.flush();
//          json.close();
//          
//          output.flush();
//          
//          System.out.println("FLUSHED!!");
//          System.out.flush();
//          
//          
//          try {
//            Thread.sleep(2000);
//          } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//          }
//        }
//        cos.done();
//      }
//    };
//    return Response.ok()
//        .header("Transfer-Encoding", "chunked")
//        .entity(stream)
//        .type(MediaType.APPLICATION_JSON)
//        .build();
//  }
}

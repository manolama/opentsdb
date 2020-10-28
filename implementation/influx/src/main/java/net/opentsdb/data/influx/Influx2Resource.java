package net.opentsdb.data.influx;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.resources.Resource;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;

@Path("put/influx2")
public class Influx2Resource extends BaseTSDBPlugin implements Resource {
  private static final Logger LOG = LoggerFactory.getLogger(Influx2Resource.class);
  
  public static final String TYPE = Influx2Resource.class.getSimpleName();
  
  private WritableTimeSeriesDataStore data_store;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    // TODO 
    this.tsdb = tsdb;
    this.id = id;
    
    WritableTimeSeriesDataStoreFactory factory = tsdb.getRegistry().getDefaultPlugin(WritableTimeSeriesDataStoreFactory.class);
    if (factory == null) {
      throw new IllegalStateException("Unable to find a default data store factory.");
    }
    data_store = factory.newStoreInstance(tsdb, null);
    if (data_store == null) {
      throw new IllegalStateException("Unable to find a default data store.");
    }
    LOG.info("********** Found data store: " + data_store);
    return Deferred.fromResult(null);
  }
  
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.TEXT_PLAIN)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request) throws Exception {
    // TODO - pool and stuff.
    Influx2 parser = new Influx2();
    byte[] buf = new byte[request.getContentLength()];
    request.getInputStream().read(buf);
    parser.setBuffer(buf);
    data_store.write(null, parser, null);
    return Response.ok()
        .entity("Thanks!")
        .header("Content-Type", "text/plain")
        .build();
  }

  @Override
  public String type() {
    return TYPE;
  }
  
}

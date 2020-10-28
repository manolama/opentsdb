package net.opentsdb.data.influx;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.servlet.AsyncContext;
import javax.servlet.ReadListener;
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

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.resources.Resource;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;

@Path("put/influx2/write")
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
    InputStream stream = request.getInputStream();
    LOG.info("REQUEST: " + request);
    String encoding = request.getHeader("Content-Encoding");
    LOG.info("ENCODING: " + request.getHeader("Content-Encoding"));
    if (!Strings.isNullOrEmpty(encoding)) {
      if (encoding.equalsIgnoreCase("gzip")) {
        stream = new GZIPInputStream(stream);
      }
    }
    
    parser.setInputStream(stream);
    
    String[] db = request.getParameterValues("db");
    if (db != null && db.length > 0) {
      parser.setNamespace(db[0]);
    }
    data_store.write(null, parser, null);
    parser.close();
    LOG.info("QUERY PARANS: " + request.getParameterMap());
    LOG.info("#@@@@@@@@@@ DB: " + new String(parser.namespaceBuffer()));
    return Response.noContent()
        .header("Content-Type", "text/plain")
        .build();
  }

  @Override
  public String type() {
    return TYPE;
  }
  
}

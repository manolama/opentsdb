package net.opentsdb.servlet.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.IncomingDataPoint;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.JSON;

@Path("api/put")
public class PutRpc {
  private static TypeReference<ArrayList<IncomingDataPoint>> TR_INCOMING =
      new TypeReference<ArrayList<IncomingDataPoint>>() {};
      
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request) throws Exception {
    
    Object obj = servlet_config.getServletContext()
        .getAttribute(OpenTSDBApplication.TSD_ATTRIBUTE);
    if (obj == null) {
      throw new WebApplicationException("Unable to pull TSDB instance from "
          + "servlet context.",
          Response.Status.INTERNAL_SERVER_ERROR);
    } else if (!(obj instanceof TSDB)) {
      throw new WebApplicationException("Object stored for as the TSDB was "
          + "of the wrong type: " + obj.getClass(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    final TSDB tsdb = (TSDB) obj;
    
    List<IncomingDataPoint> dps = JSON.parseToObject(request.getInputStream(), TR_INCOMING);
    
    class IDWrapper implements TimeSeriesStringId {
      final IncomingDataPoint dp;
      IDWrapper(final IncomingDataPoint dp) {
        this.dp = dp;
      }
      
      @Override
      public boolean encoded() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public TypeToken<? extends TimeSeriesId> type() {
        return Const.TS_STRING_ID;
      }

      @Override
      public int compareTo(TimeSeriesStringId o) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public String alias() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String namespace() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public String metric() {
        return dp.getMetric();
      }

      @Override
      public Map<String, String> tags() {
        return dp.getTags();
      }

      @Override
      public List<String> aggregatedTags() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public List<String> disjointTags() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Set<String> uniqueIds() {
        // TODO Auto-generated method stub
        return null;
      }
      
    }

    class ValueWrapper implements TimeSeriesValue<NumericType>, NumericType {
      final long value;
      final boolean is_integer;
      final TimeStamp ts;
      
      ValueWrapper(final IncomingDataPoint dp) {
        if (NumericType.looksLikeInteger(dp.getValue())) {
          value = NumericType.parseLong(dp.getValue());
          is_integer = true;
        } else {
          value = Double.doubleToLongBits(Double.parseDouble(dp.getValue()));
          is_integer = false;
        }
        ts = new MillisecondTimeStamp(dp.getTimestamp());
      }
      
      @Override
      public boolean isInteger() {
        return is_integer;
      }

      @Override
      public long longValue() {
        // TODO Auto-generated method stub
        return value;
      }

      @Override
      public double doubleValue() {
        // TODO Auto-generated method stub
        return Double.longBitsToDouble(value);
      }

      @Override
      public double toDouble() {
        if (is_integer) {
          return (longValue());
        }
        return doubleValue();
      }

      @Override
      public TimeStamp timestamp() {
        return ts;
      }

      @Override
      public NumericType value() {
        return this;
      }

      @Override
      public TypeToken<NumericType> type() {
        return NumericType.TYPE;
      }
      
    }
    
    TimeSeriesDataStore store = tsdb.getRegistry().getDefaultStore();
    
    for (final IncomingDataPoint dp : dps) {
      store.write(new IDWrapper(dp), new ValueWrapper(dp), null);
    }
    return Response.status(204).build();
  }
  
}

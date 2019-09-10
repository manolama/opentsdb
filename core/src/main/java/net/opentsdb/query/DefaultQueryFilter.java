package net.opentsdb.query;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.TimeSeriesQuery.CacheMode;
import net.opentsdb.utils.Pair;

public class DefaultQueryFilter extends BaseTSDBPlugin implements QueryFilter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultQueryFilter.class);
  
  private static final TypeReference<Map<String, Map<String, String>>> MAP_OF_MAPS =
      new TypeReference<Map<String, Map<String, String>>>() { };
      private static final TypeReference<Map<String, Map<String, Map<String, String>>>> MAP_OF_MAP_OF_MAPS =
          new TypeReference<Map<String, Map<String, Map<String, String>>>>() { };
  private static final String HEADER_KEY = "tsd.queryfilter.filter.headers";
  private static final String USER_KEY = "tsd.queryfilter.filter.users";
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    
    if (!tsdb.getConfig().hasProperty(HEADER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(HEADER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MAP_OF_MAP_OF_MAPS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    if (!tsdb.getConfig().hasProperty(USER_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(USER_KEY)
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(MAP_OF_MAPS)
          .setSource(this.getClass().toString())
          .isDynamic()
          .build());
    }
    
    return Deferred.fromResult(null);
  }
  
  @Override
  public TimeSeriesQuery filter(final TimeSeriesQuery query, 
      final AuthState auth_state, 
      final Map<String, String> headers) {
    SemanticQuery.Builder builder = null;
    Map<String, Map<String, Map<String, String>>> hdr_filter = tsdb.getConfig().getTyped(HEADER_KEY, MAP_OF_MAP_OF_MAPS);
    if (!hdr_filter.isEmpty()) {
      for (final Entry<String, String> entry : headers.entrySet()) {
        Map<String, Map<String, String>> header_filter = hdr_filter.get(entry.getKey());
        if (header_filter != null) {
          Map<String, String> values = header_filter.get(entry.getValue());
          if (values != null) {
            // OVERRIDE the query!
            if (builder == null) {
              builder = ((SemanticQuery) query).toBuilder();
            }
            String ov = values.get("cacheMode");
            if (ov != null && query.getCacheMode() != CacheMode.valueOf(ov)) {
              builder.setCacheMode(CacheMode.valueOf(ov));
              LOG.trace("Overriding cache mode for header: " + entry.getKey() + ":" + entry.getValue() + " to " + CacheMode.valueOf(ov));
            }
          }
        }
      }
    }
    
    Map<String, Map<String, String>> user_filters = tsdb.getConfig().getTyped(USER_KEY, MAP_OF_MAPS);
    if (user_filters != null) {
      final String user = auth_state != null && auth_state.getPrincipal() != null ? auth_state.getPrincipal().getName() : "Unknown";
      Map<String, String> filter = user_filters.get(user);
      if (filter != null) {
        // OVERRIDE the query!
        if (builder == null) {
          builder = ((SemanticQuery) query).toBuilder();
        }
        String ov = filter.get("cacheMode");
        if (ov != null && query.getCacheMode() != CacheMode.valueOf(ov)) {
          builder.setCacheMode(CacheMode.valueOf(ov));
          if (LOG.isTraceEnabled()) {
            LOG.trace("Overriding cache mode for user: " + user + " to " + CacheMode.valueOf(ov));
          }
        }
      }
    }
    
    return builder != null ? builder.build() : query;
  }

  @Override
  public String type() {
    // TODO Auto-generated method stub
    return "DefaultQueryFilter";
  }

  
}

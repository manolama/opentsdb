package net.opentsdb.search;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchQuery.SearchResults;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.stumbleupon.async.Deferred;

public class SearchES extends Search {
  private static final Logger LOG = LoggerFactory.getLogger(SearchES.class);
  
  private final ImmutableList<String> hosts;
  private final HttpClient httpClient;
  private final String index;
  private final String tsmeta_type;
  private int host_index;
  
  public SearchES(){
    PoolingClientConnectionManager http_pool = new PoolingClientConnectionManager();
    http_pool.setDefaultMaxPerRoute(25);
    http_pool.setMaxTotal(50);
    this.httpClient = new DefaultHttpClient(http_pool);
    this.index = "opentsdb";
    this.tsmeta_type = "tsmetadata";
    
    String[] splits = "http://wtdb-1-3.phx3.llnw.net:9200/".split(";");
    Builder<String> host_list = ImmutableList.<String>builder();
    for (String h : splits){
      host_list.add(h);
    }
    hosts = host_list.build();
    host_index = 0;
  }
  
  @Override
  public Deferred<Boolean> indexTimeSeriesMeta(final TimeSeriesMeta meta) {
    try {
      // the ID is going to be the TSUID      
      HttpPost httpPost = new HttpPost(getHost() + this.index + "/" + 
          this.tsmeta_type + "/" + meta.getUID() + "?replication=async");
      HttpContext context = new BasicHttpContext();
      
      JSON codec = new JSON(meta);
      httpPost.setEntity(new StringEntity(codec.getJsonString()));
      
      HttpResponse response = this.httpClient.execute(httpPost, context);
      HttpEntity entity = response.getEntity();
      if (entity != null) {
          // do something useful with the entity
        // ensure the connection gets released to the manager
        EntityUtils.consume(entity);
      }
      if (response.getStatusLine().getStatusCode() == 200 || 
          response.getStatusLine().getStatusCode() == 201){
        LOG.trace("Successfully indexed TSUID [" + meta.getUID() + "]");
        return Deferred.fromResult(true);
      }
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClientProtocolException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return Deferred.fromResult(false);
  }

  @Override
  public Deferred<Boolean> indexAnnotation(final Annotation note) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void reindexTSUIDs(final TSDB tsdb) {
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    scanner.setFamily(TsdbStore.toBytes("name"));
    // the ID is going to be the TSUID      
    HttpPost httpPost = new HttpPost(getHost() + this.index + "/" + 
        this.tsmeta_type + "/_bulk?replication=async");
    HttpContext context = new BasicHttpContext();
    StringBuilder post_data = new StringBuilder();
    JSON codec;
    
    try {
      scanner = tsdb.uid_storage.openScanner(scanner);

      final int limit = 100; 
      long count=0;
      ArrayList<Document> docs = new ArrayList<Document>();
      
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = tsdb.uid_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            if (TsdbStore.fromBytes(cell.qualifier()).compareTo("ts_meta") == 0){
              final String uid = UniqueId.IDtoString(cell.key());
              
              TimeSeriesMeta meta = tsdb.getTimeSeriesMeta(cell.key());
              if (meta == null){
                LOG.warn(String.format("Error retrieving TSUID metadata [%s]", uid));
                continue;
              }
              count++;
              codec = new JSON(meta);
              
              post_data.append("{\"index\":{\"_id\":\"").append(meta.getUID()).append("\"}}\n");
              post_data.append(codec.getJsonString()).append("\n");
              
              // flush every X documents
              if (count % limit == 0){
              LOG.debug(String.format("Flushing [%d] docs of [%d] to index", limit, count));
              
               try {
                 httpPost.setEntity(new StringEntity(post_data.toString()));
                 HttpResponse response = this.httpClient.execute(httpPost, context);
                 HttpEntity entity = response.getEntity();
                 if (entity != null) {
                   // ensure the connection gets released to the manager

                   if (response.getStatusLine().getStatusCode() == 200 || 
                       response.getStatusLine().getStatusCode() == 201){
                   }else{
                     LOG.warn("Error indexing data: " + response.getStatusLine().toString());
                   }
                   EntityUtils.consume(entity);
                 }
               } catch (UnsupportedEncodingException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               } catch (ClientProtocolException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               } catch (IOException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               }
               
               // reset the post data
               post_data = new StringBuilder();
              }
            }
          }
        }
      }
      
      // anything leftover, flush it!
      if (docs.size() > 0){
        LOG.debug(String.format("Flushing [%d] remaining docs to index", docs.size()));
        try {
          HttpResponse response = this.httpClient.execute(httpPost, context);
          HttpEntity entity = response.getEntity();
          if (entity != null) {
            // ensure the connection gets released to the manager
            EntityUtils.consume(entity);
          }
          if (response.getStatusLine().getStatusCode() == 200 || 
              response.getStatusLine().getStatusCode() == 201){
          }
        } catch (UnsupportedEncodingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (ClientProtocolException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      
      LOG.info(String.format("Finished reindexing [%d] TSUID metadata", count));
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  @Override
  public SearchResults searchTSMeta(SearchQuery query) {
    try {
      String uri = getHost() + this.index + "/" + this.tsmeta_type + "/_search?";
      int params = 0;
      if (query.getLimit() > 0){
        uri += "size=" + query.getLimit();
        params++;
      }
      if (query.getStartIndex() > 0){
        uri += (params > 0 ? "&" : "") + "from=" + query.getStartIndex();
        params++;
      }
      //uri += (params > 0 ? "&" : "") + "q=" + query.getQuery();
      
      // we're gonna dump the query in a JSON packet since the URI encoding can be finicky
      String json = "{\"query\":{\"query_string\":{\"query\":\"" + query.getQuery();
      json += "\"}}}";
      LOG.trace("Calling URI: " + uri);
      
      HttpPost httpPost = new HttpPost(uri.toString());
      httpPost.setEntity(new StringEntity(json));
      HttpContext context = new BasicHttpContext();
      HttpResponse response = this.httpClient.execute(httpPost, context);
      HttpEntity entity = response.getEntity();
      JSON codec = new JSON();
      SearchResults results = new SearchResults(query);
      if (entity != null) {
        
        JsonParser jp = codec.parseStream(entity.getContent());
        if (jp == null){
          LOG.error("Unable to parse results from ES");
          EntityUtils.consume(entity);
          return null;
        }
        
        JsonToken next;
        
        next = jp.nextToken();
        if (next != JsonToken.START_OBJECT) {
          LOG.error("Error: root should be object: quiting.");
          EntityUtils.consume(entity);
          return null;
        }
        
        // loop through the JSON structure
        String parent = "";
        String last = "";
        while (jp.nextToken() != null) {
          String fieldName = jp.getCurrentName();
          if (fieldName != null)
            last = fieldName;
          
          if (jp.getCurrentToken() == JsonToken.START_ARRAY || 
              jp.getCurrentToken() == JsonToken.START_OBJECT)
            parent = last;
          
          if (fieldName != null && fieldName.equals("_source")) {
            if (jp.nextToken() == JsonToken.START_OBJECT) {
              if (results.ts_meta == null)
                results.ts_meta = new ArrayList<TimeSeriesMeta>();
              results.ts_meta.add((TimeSeriesMeta)jp.readValueAs(TimeSeriesMeta.class));
            }else
              LOG.warn("Invalid _source value from ES, should have been a START_OBJECT");
          }else if (fieldName != null && jp.getCurrentToken() != JsonToken.FIELD_NAME &&
              parent.equals("hits") && fieldName.equals("total")){
            LOG.trace("Total hits: [" + jp.getValueAsInt() + "]");
            results.total_hits = jp.getValueAsInt();
          }//else if (fieldName != null)
           //LOG.trace("Unprocessed property: " + fieldName + "  of type [" + jp.getCurrentToken().toString() + "]");
        }
        
        // ensure the connection gets released to the manager
        EntityUtils.consume(entity);
      }
      
      return results;      
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClientProtocolException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private final String getHost(){
    this.host_index = this.host_index >= this.hosts.size() - 1 ? 0 : this.host_index + 1;
    return hosts.get(host_index);
  }
}

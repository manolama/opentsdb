package net.opentsdb.search;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will handle writing data to Lucene
 *
 */
public class SearchIndexer {
  private static final Logger LOG = LoggerFactory.getLogger(SearchIndexer.class);
  
  private final String directory;
  private IndexWriter idx = null;
  private Directory idx_directory = null;
  private int flush_limit;

  public SearchIndexer(final String directory){
    this.directory = directory;
    this.flush_limit = 0;
  }
  
  public final boolean index(final Document doc, final String update_field){
    if (doc == null)
      return false;
    
    List<Document> docs = new ArrayList<Document>();
    docs.add(doc);
    return index(docs, update_field);
  }
  
  /**
   * Adds the given documents to the index. If update_field is set, it will attempt
   * to update an existing doc in the index.
   * @param docs List of documents to index
   * @param update_field The name of the field to search to perform an update
   * @return
   */
  public final boolean index(final List<Document> docs, final String update_field){
    if (docs.size() < 1)
      return true;
    
    if (!this.openWriter()){
      return false;
    }
    
    int count = 0;
    for (Document doc : docs){
      try {
        if (update_field == null || update_field.isEmpty()){
            idx.addDocument(doc);
        }else{
          final String update_term = doc.get(update_field);
          if (update_term == null){
            LOG.trace(String.format("Document did not have update field [%s]", update_field));
            idx.addDocument(doc);
          }else{
            Term term = new Term(update_field, doc.get(update_field));
            idx.updateDocument(term, doc);
          }
        }
        
        // flush every so often so we aren't maintaining crap forever
        if (this.flush_limit > 0 && count >= this.flush_limit){
          idx.commit();
          count = 0;
        }else
          count++;
      } catch (CorruptIndexException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    return this.closeWriter();
  }
  
  /**
   * Attempts to remove the given document from the index
   * @param field The name of the field to search for, e.g. a UID
   * @param uid The matching value to delete. Should be a UID
   * @return True if the delete was successful, false if there was an error
   */
  public final boolean deleteDoc(final String field, final String uid){
    if (!this.openWriter()){
      return false;
    }
    
    Term term = new Term(field, uid);
    try {
      idx.deleteDocuments(term);
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return this.closeWriter();
  }
  
  public final boolean reindexTSUIDs(final TSDB tsdb){
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    scanner.setFamily(TsdbStore.toBytes("name"));
    DefaultHttpClient httpclient = new DefaultHttpClient();
    String uri_base = "http://wtdb-1-3.phx3.llnw.net:9200/opentsdb/tsmetadata/";
    String uri_suffix = "?replication=async";
    HttpPost httpPost = new HttpPost(uri_base);
    HttpResponse response2 = null;
    HttpEntity entity2 = null;
    StringBuilder post_data = new StringBuilder();
    try {
      scanner = tsdb.uid_storage.openScanner(scanner);

      final int limit = 10; 
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

              //docs.add(meta.buildLuceneDoc());
              count++;
              
              post_data.append("{\"index\":{\"_id\":\"").append(meta.getUID()).append("\"}}\n");
              post_data.append(JSON.serializeToString(meta)).append("\n");
              
              // flush every X documents
              if (count % limit == 0){
                //LOG.debug(String.format("Flushing [%d] docs to index", docs.size()));
                //this.index(docs, "tsuid");
                //docs.clear();
                
              LOG.debug(String.format("Flushing [%d] docs to index", count));
              
               try {
                 httpPost.setURI(new URI(uri_base + "_bulk" + uri_suffix));
                 
                 httpPost.setEntity(new StringEntity(post_data.toString()));
                 response2 = httpclient.execute(httpPost);
                 LOG.debug(response2.getStatusLine().toString());
                 entity2 = response2.getEntity();
                   // do something useful with the response body
                   // and ensure it is fully consumed
                   //EntityUtils.consume(entity2);
               } catch (UnsupportedEncodingException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               } catch (ClientProtocolException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               } catch (IOException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
               } finally{
                 if (entity2 != null)
                   entity2.consumeContent();
               }
               post_data = new StringBuilder();
               
              }
            }
          }
        }
      }
      
      // anything leftover, flush it!
//      if (docs.size() > 0){
//        LOG.debug(String.format("Flushing [%d] docs to index", docs.size()));
//        this.index(docs, "tsuid");
//      }
      
      LOG.info(String.format("Indexed [%d] TSUID metadata", count));
      return true;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
// PRIVATES -----------------------------------------
  
  private final boolean openWriter(){
    if (this.directory == null || this.directory.isEmpty()){
      LOG.error("Directory path for the index was empty");
      return false;
    }
    
    if (this.idx_directory == null){
      try {
        this.idx_directory = FSDirectory.open(new File(this.directory));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return false;
      }
    }
    
    try {
      this.idx = new IndexWriter(idx_directory, new StandardAnalyzer(Version.LUCENE_36), 
          IndexWriter.MaxFieldLength.LIMITED);
      return true;
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (LockObtainFailedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  private final boolean closeWriter(){
    if (this.idx == null){
      LOG.error("Indexer was null");
      return false;
    }
    
    try {
      
      // todo - research merge some more
      
      idx.commit();     
      idx.close();
      return true;
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;    
  }
}

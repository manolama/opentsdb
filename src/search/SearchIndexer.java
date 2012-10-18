package net.opentsdb.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueId;

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

  public SearchIndexer(final String directory){
    this.directory = directory;
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
    
    try {
      scanner = tsdb.uid_storage.openScanner(scanner);

      final int limit = 1000; 
      long count=0;
      ArrayList<Document> docs = new ArrayList<Document>();
      
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = tsdb.uid_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            if (TsdbStore.fromBytes(cell.qualifier()).compareTo("ts_meta") == 0){
              final String uid = UniqueId.IDtoString(cell.key());
              
              TimeSeriesMeta meta = tsdb.getTimeSeriesMeta(cell.key(), true);
              if (meta == null){
                LOG.warn(String.format("Error retrieving TSUID metadata [%s]", uid));
                continue;
              }
              
              docs.add(meta.buildLuceneDoc());
              count++;
              
              // flush every X documents
              if (count % limit == 0){
                LOG.trace(String.format("Flushing [%d] docs to index", docs.size()));
                this.index(docs, "tsuid");
                docs.clear();
              }
            }
          }
        }
      }
      
      // anything leftover, flush it!
      if (docs.size() > 0){
        LOG.debug(String.format("Flushing [%d] docs to index", docs.size()));
        this.index(docs, "tsuid");
      }
      
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

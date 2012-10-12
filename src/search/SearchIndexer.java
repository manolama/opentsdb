package net.opentsdb.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
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
          }
          Term term = new Term(update_field, doc.get(update_field));
          idx.updateDocument(term, doc);
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

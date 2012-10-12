package net.opentsdb.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the search for Lucene
 */
public class Searcher {
  private static final Logger LOG = LoggerFactory.getLogger(Searcher.class);
  
  private final String directory;
  private Directory idx_directory = null;
  private IndexSearcher searcher = null;
  private final QueryParser parser;
  
  public Searcher(final String directory){
    this.directory = directory;
    this.parser = new QueryParser(Version.LUCENE_36, "content", 
        new StandardAnalyzer(Version.LUCENE_36));
  }
  
  public final ArrayList<String> searchTSUIDs(final SearchQuery query){
    if (query.getQuery() == null || query.getQuery().isEmpty())
      return null;
    
    if (!this.checkSearcher()){
      return null;
    }
    
    TopDocs hits = this.search(query.getQuery(), Integer.MAX_VALUE, null);
    
    if (hits == null){
      LOG.error(String.format("Unable to execute query [%s]", query.getQuery()));
      return null;
    }
    
    query.setTotal_hits(hits.totalHits);
    if (hits.totalHits < 1)      
      return new ArrayList<String>();
    
    final int page = query.getPage();
    final int limit = query.getLimit();
    
    if (hits.totalHits < (page * limit)){
      query.setError(String.format("Page [%d] starting at result [%d] is greater than total results [%d]", 
          page, (page * limit), hits.totalHits));
      LOG.warn(query.getError());
      return null;
    }
    
    // return an array of TSUIDs
    ArrayList<String> tsuids = new ArrayList<String>();
    for(int i = (page * limit); i < ((page + 1) * limit); i++) {
      try {
        final String tsuid = searcher.doc(hits.scoreDocs[i].doc).get("tsuid");
        if (tsuid != null)
          tsuids.add(tsuid);
        // bail if we exceed the bounds
        if (i+2 > hits.totalHits)
          break;
      } catch (CorruptIndexException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return tsuids;
  }
  
  public final ArrayList<Map<String, Object>> searchShortMeta(final SearchQuery query){
    if (query.getQuery() == null || query.getQuery().isEmpty())
      return null;
    
    if (!this.checkSearcher()){
      return null;
    }

    TopDocs hits = this.search(query.getQuery(), Integer.MAX_VALUE, null);
    
    if (hits == null){
      LOG.error(String.format("Unable to execute query [%s]", query.getQuery()));
      return null;
    }

    query.setTotal_hits(hits.totalHits);
    if (hits.totalHits < 1)      
      return new ArrayList<Map<String, Object>>();
    
    final int page = query.getPage();
    final int limit = query.getLimit();
    
    if (hits.totalHits < (page * limit)){
      query.setError(String.format("Page [%d] starting at result [%d] is greater than total results [%d]", 
          page, (page * limit), hits.totalHits));
      LOG.warn(query.getError());
      return null;
    }
    
    // return an array of TSUIDs
    ArrayList<Map<String, Object>> metas = new ArrayList<Map<String, Object>>();
    for(int i = (page * limit); i < ((page + 1) * limit); i++) {
      try {
        final String tsuid = searcher.doc(hits.scoreDocs[i].doc).get("tsuid");
        if (tsuid == null){
          LOG.error(String.format("Unable to find TSUID for doc [%d]", hits.scoreDocs[i].doc));
          continue;
        }
        
        final String metric = searcher.doc(hits.scoreDocs[i].doc).get("metric");
        if (metric == null){
          LOG.error(String.format("Unable to find metric for doc [%d]", hits.scoreDocs[i].doc));
          continue;
        }
        
        final String[] tags = searcher.doc(hits.scoreDocs[i].doc).getValues("tags");
        if (tags == null || tags.length < 1){
          LOG.error(String.format("Unable to find tags for doc [%d]", hits.scoreDocs[i].doc));
          continue;
        }
        
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("tsuid", tsuid);
        meta.put("metric", metric);
        
        ArrayList<Map<String, String>> tag_list = new ArrayList<Map<String, String>>();
        for (String tag : tags){
          final String[] split = tag.split(" ");
          if (split.length != 2){
            LOG.error(String.format("Unable to split indexed tag [%s] from doc [%d]", tag, hits.scoreDocs[i].doc));
            continue;
          }
          
          Map<String, String> t = new HashMap<String, String>();
          t.put(split[0], split[1]);
          tag_list.add(t);
        }
        meta.put("tags", tag_list);
        metas.add(meta);
        
        // bail if we exceed the bounds
        if (i+2 > hits.totalHits)
          break;
      } catch (CorruptIndexException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return metas;
  }

// PRIVATES -----------------------------------------
  
  /**
   * If there were index updates, closes and re-opens the searcher, and opens the searcher
   * if it wasn't already
   */
  private final boolean checkSearcher(){
    if (searcher == null)
      return this.openSearcher();
    
    try {
      if (searcher.getIndexReader().isCurrent())
        return true;
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
    
    this.closeSearcher();
    
    return this.openSearcher();
  }
  
  private final boolean openSearcher(){
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
      this.searcher = new IndexSearcher(this.idx_directory, true);      
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

  private final boolean closeSearcher(){
    if (this.searcher == null){
      LOG.error("Searcher was null");
      return false;
    }
    
    try {
      searcher.close();
      searcher = null;
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

  /**
   * Runs a standard string query search
   * @param query
   * @return
   */
  private final TopDocs search(final String query, final int limit, final ScoreDoc last_result){
    try {
      Query q = parser.parse(query);
      
      if (last_result == null)
        return searcher.search(new ConstantScoreQuery(q), limit);
      else
        return searcher.searchAfter(last_result, new ConstantScoreQuery(q), limit);
      
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
}

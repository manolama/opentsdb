package net.opentsdb.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.TermSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.regex.RegexQuery;
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
  // Docs say that the parser isn't thread safe, so we better open one per call
  //private final QueryParser parser;
  
  public Searcher(final String directory){
    this.directory = directory;
//    this.parser = new QueryParser(Version.LUCENE_36, "content", 
//        new StandardAnalyzer(Version.LUCENE_36));
//    this.parser.setAllowLeadingWildcard(true);
  }
  
  public final ArrayList<String> searchTSUIDs(final SearchQuery query){
    if (!this.checkSearcher()){
      return null;
    }
    
    TopDocs hits = this.search(query, null);
    
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
          tsuids.add(tsuid.toUpperCase());
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
    if (!this.checkSearcher()){
      return null;
    }

    TopDocs hits = this.search(query, null);
    
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
      Map<String, Object> meta = this.getMeta(hits.scoreDocs[i].doc);
      if (meta != null)
        metas.add(meta);
      // bail if we exceed the bounds
      if (i+2 > hits.totalHits)
        break;
    }
    return metas;
  }

  public final Map<String, Object> groupBy(final SearchQuery query){
    if (!this.checkSearcher()){
      return null;
    }
    if (query.getGroup() == null){
      LOG.error("Group value was null");
      return null;
    }
    SortField group_sort = new SortField(query.getGroup(), SortField.STRING);
    SortField doc_sort = null;
    if (query.getSubGroup() != null)
      doc_sort = new SortField(query.getSubGroup(), SortField.STRING);
    final int page = query.getPage();
    final int limit = query.getLimit();
    
    try {
      TermFirstPassGroupingCollector c1 = new TermFirstPassGroupingCollector(
          query.getGroup(), new Sort(group_sort), searcher.maxDoc());
      
      boolean cacheScores = true;
      double maxCacheRAMMB = 256.0;
      
      CachingCollector cachedCollector = CachingCollector.create(c1, cacheScores, maxCacheRAMMB);
      Query q = NumericRangeQuery.newDoubleRange("created", 0d, Double.MAX_VALUE, true, true);
      searcher.search(q, cachedCollector);
      
      Collection<SearchGroup<String>> topGroups = c1.getTopGroups(0, true);
      
      if (topGroups == null) {
        // No groups matched
        LOG.warn(String.format("No groups matched for field [%s]", query.getGroup()));
        return null;
      }
      System.out.println(String.format("have [%d] groups", topGroups.size()));
      
      TermSecondPassGroupingCollector c2 = new TermSecondPassGroupingCollector(
          "host", topGroups, new Sort(group_sort), 
          new Sort(doc_sort), 500, false, false, false);

      //Optionally compute total group count
      TermAllGroupsCollector allGroupsCollector = null;
      if (true) {
        allGroupsCollector = new TermAllGroupsCollector("host");
        //c2 = (TermSecondPassGroupingCollector) MultiCollector.wrap(c2, allGroupsCollector);
      }

      if (cachedCollector.isCached()) {
        // Cache fit within maxCacheRAMMB, so we can replay it:
        cachedCollector.replay(c2);
      } else {
        // Cache was too large; must re-execute query:
        searcher.search(q, c2);
      }

      TopGroups<String> groupsResult = c2.getTopGroups(0);
      groupsResult = new TopGroups<String>(groupsResult, allGroupsCollector.getGroupCount());               
//        System.out.println("Total grouped docs: " + groupsResult.totalGroupedHitCount);

      Map<String, Object> group_map = new HashMap<String, Object>();
      for(int i = (page * limit); i < ((page + 1) * limit); i++) {
        if (i >= groupsResult.groups.length)
          break;
        
        GroupDocs<String> doc = groupsResult.groups[i];
        if (doc.groupValue == null || doc.groupValue.isEmpty()){
          LOG.warn("Empty group string encountered");
          continue;
        }
        
        // if the user only wants the groups, give em the groups
        if (query.getGroupOnly()){
          group_map.put(doc.groupValue, doc.scoreDocs.length);
          continue;
        }
        
        ArrayList<Map<String, Object>> metas = new ArrayList<Map<String, Object>>();
        for (ScoreDoc sd : doc.scoreDocs){
          Map<String, Object> meta = this.getMeta(sd.doc);
          if (meta != null){
            metas.add(meta);
          }else{
            LOG.error(String.format("Unable to get metadata for [%s]", doc.groupValue));
          }
        }
        
        if (metas.size() < 1){
          LOG.warn(String.format("No sub-groups found for [%s] docs [%d]", doc.groupValue, doc.scoreDocs.length));
        }else
          group_map.put(doc.groupValue, metas);
      }
      
      // set query vars
      query.setTotalGroups(topGroups.size());
      query.setTotal_hits(groupsResult.totalHitCount);
      return group_map;
      
//        int count = 0;
//        for (int i = (page * limit); i < ((page  + 1) * limit); i++){
//          GroupDocs<String> doc = groupsResult.groups[i];
//          System.out.println("- " + doc.groupValue);
//          count++;
//        }
//        System.out.println("Dumped [" + count + "] groups");
      
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
  
  public final HashSet<String> getTerms(final SearchQuery query){
    if (!this.checkSearcher()){
      return null;
    }
    if (query.getGroup() == null){
      LOG.error("Group value was null");
      return null;
    }
    
    TermEnum terms;
    try {
      terms = searcher.getIndexReader().terms();
      HashSet<String> uniqueTerms = new HashSet<String>();
      while (terms.next()) {
        final Term term = terms.term();
        if (term.field().equals(query.getGroup())) {
          uniqueTerms.add(term.text());
        }
      }
      
      query.setLimit(0);
      query.setTotal_hits(uniqueTerms.size());
      return uniqueTerms;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
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
  
  @SuppressWarnings("deprecation")
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
  private final TopDocs search(final SearchQuery query, final ScoreDoc last_result){
    try {
      Query q = null;
      QueryParser parser = new QueryParser(Version.LUCENE_36, "content", 
          new StandardAnalyzer(Version.LUCENE_36));
      parser.setAllowLeadingWildcard(true);
      parser.setLowercaseExpandedTerms(false);
      
      // if we want ALL records, do so
      if (query.getQuery().equals("*") || query.getQuery().isEmpty())
        q = NumericRangeQuery.newDoubleRange("created", 0d, Double.MAX_VALUE, true, true);
      else if (query.getRegex())
        q = new RegexQuery( new Term(query.getField(), query.getQuery()));
      else
        q = parser.parse(query.getQuery().toLowerCase());

      LOG.trace("Query: " + q.toString());
      if (last_result == null)
        return searcher.search(new ConstantScoreQuery(q), Integer.MAX_VALUE);
      else
        return searcher.searchAfter(last_result, new ConstantScoreQuery(q), Integer.MAX_VALUE);
      
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private final Map<String, Object> getMeta(final int doc_id){
    try{
      final String tsuid = searcher.doc(doc_id).get("tsuid");
      if (tsuid == null){
        LOG.error(String.format("Unable to find TSUID for doc [%d]", doc_id));
        return null;
      }
      
      final String metric = searcher.doc(doc_id).get("metric");
      if (metric == null){
        LOG.error(String.format("Unable to find metric for doc [%d]", doc_id));
        return null;
      }
      
      final String[] tags = searcher.doc(doc_id).getValues("tags");
      if (tags == null || tags.length < 1){
        LOG.error(String.format("Unable to find tags for doc [%d]", doc_id));
        return null;
      }
      
      Map<String, Object> meta = new HashMap<String, Object>();
      meta.put("tsuid", tsuid.toUpperCase());
      meta.put("metric", metric);
      
      HashMap<String, String> tag_list = new HashMap<String, String>();
      for (String tag : tags){
        final String[] split = tag.split("=");
        if (split.length != 2){
          LOG.error(String.format("Unable to split indexed tag [%s] from doc [%d]", tag, doc_id));
          continue;
        }
        
        tag_list.put(split[0], split[1]);
      }
      meta.put("tags", tag_list);
      return meta;
    } catch (CorruptIndexException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }
}

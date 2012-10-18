package net.opentsdb.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;

public final class SearchManager extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(SearchManager.class);
  private final TSDB tsdb;
  
  public SearchManager(final TSDB tsdb){
    this.tsdb = tsdb;
  }
  
  public void run(){
    while(true){
      LOG.info("Reindexing metadata");
      tsdb.meta_search_writer.reindexTSUIDs(tsdb);
      LOG.info("Finished reindexing metadata");
      
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}

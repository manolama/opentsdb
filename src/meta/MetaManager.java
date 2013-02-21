package net.opentsdb.meta;

import java.util.ArrayList;

import net.opentsdb.core.TSDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetaManager extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(MetaManager.class);
  private final TSDB tsdb;
  
  public MetaManager(final TSDB tsdb){
    this.tsdb = tsdb;
  }
  
  public void run(){
    while(true){
      // load all trees
      ArrayList<MetaTree> trees = MetaTree.GetTrees(tsdb.uid_storage);
      if (trees.size() < 1){
        LOG.trace("No trees found, sleeping and we'll try again");
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          return;
        }
        continue;
      }
      
      for (MetaTree tree : trees){
        long start = System.currentTimeMillis() / 1000;
        LOG.info("Rebuilding Tree [" + tree.getTree_id() + "]");
        tree.Reindex(tsdb);
        LOG.info("Finished reindexing tree [" + tree.getTree_id() + "] in [" 
            + ((System.currentTimeMillis() / 1000) - start) + "] seconds");
      }
      
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}

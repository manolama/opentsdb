package net.opentsdb.meta;

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
//    if (true)
//      return;
    while(true){
      LOG.info("Rebuilding Tree 1");
      long start = System.currentTimeMillis() / 1000;
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      tree.setTree_id(1);
//      if (tree.LoadTree(1)){
//        LOG.info("Loaded the tree!!!: " + tree.getName());
//        tree.Reindex(tsdb);
//      }else{
//        LOG.error("Tree #1 ain't there! Lets build it!");
        
        tree.setName("Pop/Type/Hostname/Metric");
        MetaTreeRule rule = new MetaTreeRule();
        rule = new MetaTreeRule();
        rule.setType(2);
        rule.setTree_id(1);
        rule.setField("fqdn");
        rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
        rule.setLevel(0);
        rule.setOrder(0);
        tree.AddRule(rule); 
        
        rule = new MetaTreeRule();
        rule = new MetaTreeRule();
        rule.setType(2);
        rule.setTree_id(1);
        rule.setField("host");
        rule.setRegex("^.*\\.([a-zA-Z]{3,4})[0-9]{0,1}\\..*\\..*$");
        rule.setLevel(0);
        rule.setOrder(1);
        tree.AddRule(rule); 
        
        rule = new MetaTreeRule();
        rule = new MetaTreeRule();
        rule.setType(2);
        rule.setTree_id(1);
        rule.setField("fqdn");
        rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
        rule.setLevel(1);
        rule.setOrder(0);
        tree.AddRule(rule); 
        
        rule = new MetaTreeRule();
        rule = new MetaTreeRule();
        rule.setType(2);
        rule.setTree_id(1);
        rule.setField("host");
        rule.setRegex("^([a-zA-Z]+)(\\-|[0-9])*.*\\..*$");
        rule.setLevel(1);
        rule.setOrder(1);
        tree.AddRule(rule);  
        
        rule = new MetaTreeRule();
        rule.setTree_id(1);
        rule.setType(2);
        rule.setField("fqdn");
        rule.setLevel(2);
        rule.setOrder(0);
        tree.AddRule(rule); 
        
        rule = new MetaTreeRule();
        rule.setTree_id(1);
        rule.setType(2);
        rule.setField("host");
        rule.setLevel(2);
        rule.setOrder(1);
        tree.AddRule(rule); 
        
        rule = new MetaTreeRule();
        rule.setTree_id(1);
        rule.setType(0);
        rule.setSeparator("\\.");
        rule.setLevel(3);
        rule.setOrder(0);
        tree.AddRule(rule); 
        
        if (tree.StoreTree()){
         LOG.info("Stored the Happy Little Tree #1");
         tree.Reindex(tsdb);
        }else{
          LOG.error("Failed to store the Misserable BIGASS tree #1");
          return;
        }
//      }
      LOG.info("Finished reindexing tree in: " + ((System.currentTimeMillis() / 1000) - start));
      
      try {
        Thread.sleep(60000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}

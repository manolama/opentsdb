package net.opentsdb.tsd;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.MetaTree;
import net.opentsdb.meta.MetaTree.MetaTreeBranchDisplay;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeRPC  implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(TreeRPC.class);

  public void execute(final TSDB tsdb, final HttpQuery query) {
    
    // get formatter
    TSDFormatter formatter = query.getFormatter();
    if (formatter == null)
      return;
    
    final String endpoint = query.getEndpoint();
    if (endpoint == null || endpoint.isEmpty()){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing proper endpoint");
      return;
    }
    
    int tree_id = 0;
    if (query.hasQueryStringParam("tree_id"))
      tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
    
    if (endpoint.compareTo("branch") == 0){
      
      int branch_id = 0;
      if (query.hasQueryStringParam("branch_id"))
        branch_id = Integer.parseInt(query.getQueryStringParam("branch_id"));
      
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      tree.setTree_id(tree_id);
      
      MetaTreeBranchDisplay br = tree.GetBranch(branch_id);
      JSON codec = new JSON(br);
      query.sendReply(codec.getJsonBytes());
    }else if (endpoint.compareTo("test") == 0){
      if (!query.hasQueryStringParam("tsuid")){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tsuid value");
        return;
      }
      
      String tsuid = query.getQueryStringParam("tsuid");
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      if (!tree.LoadTree(tree_id)){
        query.sendError(HttpResponseStatus.NOT_FOUND, "Tree was not found");
        return;
      }
      
      query.sendReply(tree.TestTS(tsdb, tsuid));
    }else{
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      tree.setTree_id(tree_id);
      if (!tree.LoadTree(tree_id)){
        query.sendError(HttpResponseStatus.NOT_FOUND, "Tree with ID [" + tree_id + "] not found");
      }else{
        JSON codec = new JSON(tree);
        query.sendReply(codec.getJsonBytes());
      }
    }
    
  }
}

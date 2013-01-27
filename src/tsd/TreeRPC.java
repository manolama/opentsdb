package net.opentsdb.tsd;

import java.util.HashMap;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.MetaTree;
import net.opentsdb.meta.MetaTree.MetaTreeBranchDisplay;
import net.opentsdb.meta.MetaTreeRule;
import net.opentsdb.storage.TsdbStore;

import org.jboss.netty.handler.codec.http.HttpMethod;
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
    }else if (endpoint.compareTo("rule") == 0){
      
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
  
  private final int getQSTreeID(final HttpQuery query){
    if (!query.hasQueryStringParam("tree_id")){
      LOG.debug("Missing tree_id");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree_id");
      return -1;
    }
    try{
      final int tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
      if (tree_id < 1){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid tree_id, must be greater than 0");
        return -1;
      }
      return tree_id;
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid tree_id, must be an integer");
      return - 1;
    }
  }
  
  private final int getQSLevel(final HttpQuery query){
    if (!query.hasQueryStringParam("level")){
      LOG.debug("Missing level");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing level");
      return - 1;
    }
    try{
      return Integer.parseInt(query.getQueryStringParam("level"));
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid level, must be an integer");
      return - 1;
    }
  }
  
  private final int getQSOrder(final HttpQuery query){
    if (!query.hasQueryStringParam("order")){
      LOG.debug("Missing order");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing order");
      return - 1;
    }
    try{
      return Integer.parseInt(query.getQueryStringParam("order"));
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid order, must be an integer");
      return - 1;
    }
  }
  
  private void handleRuleEdit(final TSDB tsdb, final HttpQuery query){  
    if (query.getMethod() == HttpMethod.GET) {
      int tree_id = 0;
      int level = 0;
      int order = 0;
      
      tree_id = getQSTreeID(query);
      if (tree_id < 0)
        return;
      
      level = getQSLevel(query);
      if (level < 0)
        return;
      
      order = getQSOrder(query);
      if (order < 0)
        return;
      
      MetaTreeRule rule = new MetaTreeRule(tree_id, level, order);
      if (!this.parseQueryString(tsdb, query, rule))
        return;
      
      // load tree
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      if (!tree.LoadTree(tree_id)){
        query.sendError(HttpResponseStatus.NOT_FOUND, "The given tree was not found");
        return;
      }
      
      // see if it has the rule we're dealing with
      final MetaTreeRule orule = tree.GetRule(level, order);
      if (!rule.hasChanges() && orule == null){
        query.sendError(HttpResponseStatus.NOT_FOUND, "The requested rule does not exist in this tree");
        return;
      }else if (!rule.hasChanges()){
        // we're just returning data
        JSON codec = new JSON(orule);
        query.sendReply(codec.getJsonBytes());
        return;
      }
      
      // we have a rule addition or change, so save it
      if (orule != null){
        orule.copyChanges(rule);
        rule = orule;
      }
      
      tree.AddRule(rule);
      if (!tree.StoreTree()){
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree data");
        return;
      }
      
      JSON codec = new JSON(rule);
      query.sendReply(codec.getJsonBytes());
      return;
    }else{
      // POST
      String post = query.getPostData();
      if (post == null || post.isEmpty()){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing POST content");
        return;
      }
      
      MetaTreeRule rule = new MetaTreeRule();
      JSON codec = new JSON(rule);
      if (!codec.parseObject(post)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the POST content into a rule");
        return;
      }
      
      rule = (MetaTreeRule)codec.getObject();
      
      MetaTree tree = new MetaTree(tsdb.uid_storage);
      if (!tree.LoadTree(rule.getTree_id())){
        query.sendError(HttpResponseStatus.NOT_FOUND, "The given tree was not found");
        return;
      }
      
      if (!rule.hasChanges()){
        query.sendError(HttpResponseStatus.BAD_REQUEST, 
            "The rule was missing configuration data, please supply at least a rule type and separator, field or regex");
        return;
      }
      
      // see if it has the rule we're dealing with
      final MetaTreeRule orule = tree.GetRule(rule.getLevel(), rule.getOrder());
      
      // we have a rule addition or change, so save it
      if (orule != null){
        orule.copyChanges(rule);
        rule = orule;
      }
      
      tree.AddRule(rule);
      if (!tree.StoreTree()){
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree data");
        return;
      }
      
      codec = new JSON(rule);
      query.sendReply(codec.getJsonBytes());
      return;
    }
  }
  
  private void handleRuleDelete(final TSDB tsdb, final HttpQuery query){ 
    int tree_id = 0;
    int level = 0;
    int order = 0;
    boolean delete_level = false;
    
    tree_id = getQSTreeID(query);
    if (tree_id < 0)
      return;
    
    level = getQSLevel(query);
    if (level < 0)
      return;
    
    order = getQSOrder(query);
    if (order < 0)
      return;

    if (query.hasQueryStringParam("delete_level"))
      delete_level = true;
    
    // load tree
    MetaTree tree = new MetaTree(tsdb.uid_storage);
    if (!tree.LoadTree(tree_id)){
      query.sendError(HttpResponseStatus.NOT_FOUND, "The given tree was not found");
      return;
    }  
    
    String response = tree.DeleteRule(level, order, delete_level);
    if (!response.isEmpty()){
      query.sendError(HttpResponseStatus.NOT_FOUND, response);
      return;
    }
    
    if (!tree.StoreTree()){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree data");
      return;
    }
    
    JSON codec = new JSON(tree);
    query.sendReply(codec.getJsonBytes());
  }
  
  /**
   * 
   * @param tsdb
   * @param query
   * @param rule
   * @return True if parsing was successful, false if not
   */
  private boolean parseQueryString(final TSDB tsdb, final HttpQuery query, MetaTreeRule rule){
    try{
      if (query.hasQueryStringParam("type"))
        rule.setType(Integer.parseInt(query.getQueryStringParam("type")));
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid order, must be an integer");
      return false;
    }
    
    if (query.hasQueryStringParam("field"))
      rule.setField(query.getQueryStringParam("field"));
    if (query.hasQueryStringParam("custom_field"))
      rule.setCustom_field(query.getQueryStringParam("custom_field"));
    if (query.hasQueryStringParam("regex"))
      rule.setRegex(query.getQueryStringParam("regex"));
    if (query.hasQueryStringParam("separator"))
      rule.setSeparator(query.getQueryStringParam("separator"));
    if (query.hasQueryStringParam("description"))
      rule.setDescription(query.getQueryStringParam("description"));
    if (query.hasQueryStringParam("notes"))
      rule.setNotes(query.getQueryStringParam("notes"));
    
    try{
      if (query.hasQueryStringParam("regex_group_idx"))
        rule.setRegex_group_idx(Integer.parseInt(query.getQueryStringParam("regex_group_idx")));
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid regex_group_idx, must be an integer");
      return false;
    }
    
    if (query.hasQueryStringParam("display_forma"))
      rule.setDisplay_format(query.getQueryStringParam("display_format"));
    return true;
  }
}

package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;

import net.opentsdb.core.TSDB;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.meta.MetaTree;
import net.opentsdb.meta.MetaTree.MetaTreeBranchDisplay;
import net.opentsdb.meta.MetaTreeRule;
import net.opentsdb.utils.JSON;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
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
    
    String[] path = query.explodePath();
    String root = path[0];
    LOG.debug(String.format("Root [%s] EP [%s]", root, endpoint));
    
    if (endpoint.compareTo("branch") == 0){
      // FETCH a specific branch. This will be called the most
      this.handleFetchBranch(tsdb, query);
    }else if (endpoint.compareTo("rule") == 0){
      // add/edit a rule
      this.handleRuleEdit(tsdb, query);
    }else if (path.length > 2 && path[1].compareTo("rule") == 0 && endpoint.compareTo("delete") == 0){
      // delete a rule
      this.handleRuleDelete(tsdb, query);
    }else if (root.compareTo("tree") == 0 && endpoint.compareTo("create") == 0){
      // create a new tree
      this.handleTreeCreate(tsdb, query);
    }else if (root.compareTo("tree") == 0 && endpoint.compareTo("edit") == 0){
      // edit a tree
      this.handleTreeEdit(tsdb, query);
    }else if (root.compareTo("tree") == 0 && endpoint.compareTo("delete") == 0){
      // mark a tree for deletion
      query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Not implemented yet");
    }else if (endpoint.compareTo("test") == 0){
      this.handleTest(tsdb, query);
    }else{
      this.handleDefault(tsdb, query);
    }
  }
  
  private void handleFetchBranch(final TSDB tsdb, final HttpQuery query){
    int branch_id = 0;
    int tree_id = 0;
    
    if (query.hasQueryStringParam("tree_id"))
      tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
    if (query.hasQueryStringParam("branch_id"))
      branch_id = Integer.parseInt(query.getQueryStringParam("branch_id"));
    
    if (tree_id < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree_id");
      return;
    }
    
    MetaTree tree = new MetaTree();
    tree.setTree_id(tree_id);
    
    MetaTreeBranchDisplay br;
    try {
      br = tree.GetBranch(tsdb.uid_storage, branch_id);
      if (br == null){
        LOG.warn("Unable to find branch [" + branch_id + "]");
        query.sendError(HttpResponseStatus.NOT_FOUND, "Branch was not found");
        return;
      }
      query.sendReply(JSON.serializeToBytes(br));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
  }

  private void handleTreeCreate(final TSDB tsdb, final HttpQuery query){
    MetaTree tree = new MetaTree();
    String post = query.getPostData();
    if (post != null && post.length() > 1){
      try {
        tree = (MetaTree)JSON.parseToObject(post, MetaTree.class);
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else
      this.parseTreeQueryString(tsdb, query, tree);
    
    if (tree.getTree_id() > 0){
      LOG.warn("Cannot create a tree with an ID supplied");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Cannot create a tree with a given ID, set the ID to 0");
      return;
    }
    
    // data checks
    int tree_id = MetaTree.GetNewID(tsdb.uid_storage);
    if (tree_id < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to retrieve a new ID for the tree, may have run out of IDs");
      return;
    }
    if (tree.getName() == null || tree.getName().length() < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree name");
      return;
    }
    
    tree.setTree_id(tree_id);
    tree.setCreated();
    if (!tree.StoreTree(tsdb.uid_storage)){
      LOG.error("Error saving tree [" + tree.getTree_id() + "]");
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree to storage");
      return;
    }
    
    try {
      query.sendReply(JSON.serializeToBytes(tree));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    LOG.trace("Successfully created tree [" + tree.getTree_id() + "]");
  }
  
  private void handleTreeEdit(final TSDB tsdb, final HttpQuery query){
    MetaTree tree = new MetaTree();
    String post = query.getPostData();
    if (post != null && post.length() > 1){
      try {
        tree = (MetaTree)JSON.parseToObject(post, MetaTree.class);
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else
      this.parseTreeQueryString(tsdb, query, tree);
    
    if (tree.getTree_id() < 1){
      LOG.warn("Missing tree ID");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree ID");
      return;
    }
    
    MetaTree stored_tree = new MetaTree();
    try {
      if (!stored_tree.LoadTree(tsdb.uid_storage, tree.getTree_id())){
        LOG.warn("Tree [" + tree.getTree_id() + "] does not exist in storage");
        query.sendError(HttpResponseStatus.NOT_FOUND, "Tree does not exist");
        return;
      }
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    
    stored_tree.copyChanges(tree);
    if (!stored_tree.StoreTree(tsdb.uid_storage)){
      LOG.error("Error saving tree [" + tree.getTree_id() + "]");
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree to storage");
      return;
    }
    
    try {
      query.sendReply(JSON.serializeToBytes(stored_tree));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    LOG.debug("Successfully edited tree [" + tree.getTree_id() + "]");
  }
  
  private void handleRuleEdit(final TSDB tsdb, final HttpQuery query){  
    MetaTreeRule rule = new MetaTreeRule();
    String post = query.getPostData();
    if (post != null && post.length() > 1){
      try {
        rule = (MetaTreeRule)JSON.parseToObject(post, MetaTreeRule.class);
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else{
      if (!this.parseRuleQueryString(tsdb, query, rule))
        return;
    }
    
    if (rule.getTree_id() < 1){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree ID");
      return;
    }
    
    // load tree
    MetaTree tree = new MetaTree();
    try {
      if (!tree.LoadTree(tsdb.uid_storage, rule.getTree_id())){
        query.sendError(HttpResponseStatus.NOT_FOUND, "The given tree was not found");
        return;
      }
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    
    // see if it has the rule we're dealing with
    final MetaTreeRule orule = tree.GetRule(rule.getLevel(), rule.getOrder());
    if (orule != null){
      orule.copyChanges(rule);
      rule = orule;
    }
    
    // validation
    if (rule.getType() > 0){
      if ((rule.getType() == 1 || rule.getType() == 3 || rule.getType() == 4)
          && (rule.getCustom_field() == null || rule.getCustom_field().length() < 1)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Custom field rule must have a custom field name");
        return;
      }
      
      if (rule.getType() > 2
          && (rule.getField() == null || rule.getField().length() < 1)){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Rule must have a field name");
        return;
      }
    }
    
    tree.AddRule(rule);
    if (!tree.StoreTree(tsdb.uid_storage)){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree data");
      return;
    }
    
    try {
      query.sendReply(JSON.serializeToBytes(tree));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    LOG.debug("Successfully updated rule: " + rule.toString());
    return;
  }
  
  private void handleRuleDelete(final TSDB tsdb, final HttpQuery query){ 
    boolean delete_level = false;
    MetaTreeRule rule = new MetaTreeRule();
    String post = query.getPostData();
    if (post != null && post.length() > 1){
      try {
        rule = (MetaTreeRule)JSON.parseToObject(post, MetaTreeRule.class);
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else{
      if (!this.parseRuleQueryString(tsdb, query, rule))
        return;
    }

    if (query.hasQueryStringParam("delete_level"))
      delete_level = query.parseBoolean(query.getQueryStringParam("delete_level"));
    
    // load tree
    MetaTree tree = new MetaTree();
    try {
      if (!tree.LoadTree(tsdb.uid_storage, rule.getTree_id())){
        query.sendError(HttpResponseStatus.NOT_FOUND, "The given tree was not found");
        return;
      }
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    } 
    
    String response = tree.DeleteRule(rule.getLevel(), rule.getOrder(), delete_level);
    if (!response.isEmpty()){
      query.sendError(HttpResponseStatus.NOT_FOUND, response);
      return;
    }
    
    if (!tree.StoreTree(tsdb.uid_storage)){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Error saving tree data");
      return;
    }
    
    try {
      query.sendReply(JSON.serializeToBytes(tree));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
  }
  
  private void handleTest(final TSDB tsdb, final HttpQuery query){
 // TEST a TSUID to find out where it will land on the tree
    if (!query.hasQueryStringParam("tsuid")){
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tsuid value");
      return;
    }
    
    int tree_id = 0;
    if (query.hasQueryStringParam("tree_id"))
      tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
    
    String tsuid = query.getQueryStringParam("tsuid");
    MetaTree tree = new MetaTree();
    try {
      if (!tree.LoadTree(tsdb.uid_storage, tree_id)){
        query.sendError(HttpResponseStatus.NOT_FOUND, "Tree was not found");
        return;
      }
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
    
    boolean save = query.hasQueryStringParam("save");
    try {
      query.sendReply(tree.TestTS(tsdb, tsuid, save));
    } catch (JsonParseException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (JsonMappingException e) {
      query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
      return;
    } catch (IOException e) {
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
      return;
    }
  }
  
  private void handleDefault(final TSDB tsdb, final HttpQuery query){
    int tree_id = 0;
    if (query.hasQueryStringParam("tree_id"))
      tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
    
    // DEFAULT: Return all or 1 tree meta data
    if (tree_id < 1){
      ArrayList<MetaTree> trees = MetaTree.GetTrees(tsdb.uid_storage);
      try {
        query.sendReply(JSON.serializeToBytes(trees));
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }else{
      MetaTree tree = new MetaTree();
      tree.setTree_id(tree_id);
      try {
        if (!tree.LoadTree(tsdb.uid_storage, tree_id)){
          query.sendError(HttpResponseStatus.NOT_FOUND, "Tree with ID [" + tree_id + "] not found");
        }else{
          query.sendReply(JSON.serializeToBytes(tree));
        }
      } catch (JsonParseException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (JsonMappingException e) {
        query.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage(), e.getStackTrace().toString());
        return;
      } catch (IOException e) {
        query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e.getStackTrace().toString());
        return;
      }
    }
  }
  
  private boolean parseTreeQueryString(final TSDB tsdb, final HttpQuery query, MetaTree tree){
    try{
      if (query.hasQueryStringParam("tree_id"))
        tree.setTree_id(Integer.parseInt(query.getQueryStringParam("tree_id")));
      if (query.hasQueryStringParam("name"))
        tree.setName(query.getQueryStringParam("name"));
      if (query.hasQueryStringParam("notes"))
        tree.setNotes(query.getQueryStringParam("notes"));
      if (query.hasQueryStringParam("strict_match"))
        tree.setStrict_match(query.parseBoolean(query.getQueryStringParam("strict_match")));
      return true;
    }catch (Exception e){
      LOG.error(e.toString());
      return false;
    }
  }
  
  /**
   * 
   * @param tsdb
   * @param query
   * @param rule
   * @return True if parsing was successful, false if not
   */
  private boolean parseRuleQueryString(final TSDB tsdb, final HttpQuery query, MetaTreeRule rule){
    if (!query.hasQueryStringParam("tree_id")){
      LOG.debug("Missing tree_id");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing tree_id");
      return false;
    }
    try{
      final int tree_id = Integer.parseInt(query.getQueryStringParam("tree_id"));
      if (tree_id < 1){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid tree_id, must be greater than 0");
        return false;
      }
      rule.setTree_id(tree_id);
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid tree_id, must be an integer");
      return false;
    }
    
    if (!query.hasQueryStringParam("level")){
      LOG.debug("Missing level");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing level");
      return false;
    }
    try{
      int level = Integer.parseInt(query.getQueryStringParam("level"));
      if (level < 0){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid level, must be equal to or greater than 0");
        return false;
      }
      rule.setLevel(level);
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid level, must be an integer");
      return false;
    }
    
    if (!query.hasQueryStringParam("order")){
      LOG.debug("Missing order");
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Missing order");
      return false;
    }
    try{
      int order = Integer.parseInt(query.getQueryStringParam("order"));
      if (order < 0){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid order, must be equal to or greater than 0");
        return false;
      }
      rule.setOrder(order);
    }catch (NumberFormatException nfe){
      LOG.debug(nfe.getMessage());
      query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid order, must be an integer");
      return false;
    }
    
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

// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.stumbleupon.async.Callback;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.tree.TreeRule;

/**
 * 
 * @since 2.0
 */
final class TreeRpc implements HttpRpc {

  /** The TSDB to use for storage access */
  private TSDB tsdb;
  
  /** The query to work with */
  private HttpQuery query;
  
  /** Query method via the API */
  private HttpMethod method;
  
  @Override
  public void execute(TSDB tsdb, HttpQuery query) throws IOException {
    this.tsdb = tsdb;
    this.query = query;
    method = query.getAPIMethod();
    
    // the uri will be /api/vX/tree/? or /api/tree/?
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : ""; 
    
    try {
      if (endpoint.isEmpty()) {
        handleTree();
      } else if (endpoint.toLowerCase().equals("branch")) {
        handleBranch();
      } else if (endpoint.toLowerCase().equals("rule")) {
        handleRule();
      } else if (endpoint.toLowerCase().equals("rules")) {
        handleRules();
      } else if (endpoint.toLowerCase().equals("test")) {
        handleTest();
      } else if (endpoint.toLowerCase().equals("collisions")) {
        handleCollisionNotMatched(true);
      } else if (endpoint.toLowerCase().equals("notmatched")) {
        handleCollisionNotMatched(false);
      } else {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "This endpoint is not supported");
      }
    } catch (BadRequestException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles the plain /tree endpoint CRUD
   * @throws Exception 
   * @throws BadRequestException if the request was invalid.
   */
  private void handleTree() throws Exception {
    final Tree tree;
    if (query.hasContent()) {
      tree = query.serializer().parseTreeV1();
    } else {
      tree = parseTree();
    }
    
    // if get, then we're just returning one or more trees
    if (method == HttpMethod.GET) {
      if (tree.getTreeId() == 0) {
        query.sendReply(query.serializer().formatTreesV1(
            Tree.fetchAllTrees(tsdb).joinUninterruptibly()));
      } else {
        final Tree single_tree = Tree.fetchTree(tsdb, tree.getTreeId())
          .joinUninterruptibly();
        if (single_tree == null) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree.getTreeId());
        }
        query.sendReply(query.serializer().formatTreeV1(single_tree));
      }
    } else if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      // For post or put, we're either editing a tree or creating a new one. If
      // the tree ID is missing, we need to create a new one, otherwise we edit
      // an existing tree.
      
      // if the tree ID is set, fetch, copy, save
      if (tree.getTreeId() > 0) {
        // TODO - see if the tree is loaded in memory
        try {
          if (Tree.fetchTree(tsdb, tree.getTreeId())
              .joinUninterruptibly() == null) {
            throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
                "Unable to locate tree: " + tree.getTreeId());
          } else {
            if (tree.storeTree(tsdb, (method == HttpMethod.PUT))
                .joinUninterruptibly()) {
              final Tree stored_tree = Tree.fetchTree(tsdb, tree.getTreeId())
                .joinUninterruptibly();
              query.sendReply(query.serializer().formatTreeV1(stored_tree));
            } else {
              throw new BadRequestException(
                  HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                  "Unable to save changes to tre tree: " + tree.getTreeId(),
                  "Plesae try again at a later time");
            }
          }
        } catch (IllegalStateException e) {
          query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
        } catch (IllegalArgumentException e) {
          throw new BadRequestException(e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        // create a new tree
        try {
          final int tree_id = tree.createNewTree(tsdb).joinUninterruptibly(); 
          if (tree_id > 0) {
            final Tree stored_tree = Tree.fetchTree(tsdb, tree_id)
              .joinUninterruptibly();
            query.sendReply(query.serializer().formatTreeV1(stored_tree));
          } else {
            throw new BadRequestException(
                HttpResponseStatus.INTERNAL_SERVER_ERROR, 
                "Unable to save changes to tree: " + tree.getTreeId(),
                "Plesae try again at a later time");
          }
        } catch (IllegalStateException e) {
          throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
              "Unable to create new tree", e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    } else if (method == HttpMethod.DELETE) {
      try {
        Tree.deleteTree(tsdb, tree.getTreeId()).joinUninterruptibly(); 
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
        "Unsupported HTTP request method");
    }
  }
  
  /**
   * Attempts to retrieve a single branch and return it to the user. If the 
   * requested branch doesn't exist, it returns a 404.
   * @throws BadRequestException if the request was invalid.
   */
  private void handleBranch() {
    try {
      final int tree_id = parseTreeId(false);
      final String branch_hex =
          query.getQueryStringParam("branch");
      
      // compile the branch ID. If the user did NOT supply a branch address, 
      // that would include the tree ID, then we fall back to the tree ID and
      // the root for that tree
      final byte[] branch_id;
      if (branch_hex == null || branch_hex.isEmpty()) {
        if (tree_id < 1) {
          throw new IllegalArgumentException(
              "Missing or invalid branch and tree IDs");
        }
        branch_id = Tree.idToBytes(tree_id);
      } else {
        branch_id = Branch.stringToId(branch_hex);
      }
      
      class MyErrBack implements Callback<Object, Exception> {

        @Override
        public Object call(Exception e) throws Exception {
          e.printStackTrace();
          return null;
        }
        
      }
      
      System.out.println("Fetching branch");
      // TODO - fix the "load_uids" stuff. For some reason it hangs when trying
      // to get the data from HBase
      final Branch branch = Branch.fetchBranch(tsdb, branch_id, true).addErrback(new MyErrBack())
        .join();
      if (branch == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate branch '" + Branch.idToString(branch_id) + 
            "' for tree '" + Tree.bytesToId(branch_id) + "'");
      }
      System.out.println("Got branch: " + branch);
      query.sendReply(query.serializer().formatBranchV1(branch));
    } catch (NumberFormatException nfe) {
      throw new BadRequestException("Unable to parse 'tree' value");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Handles the CRUD calls for a single rule, enabling adding, editing or 
   * deleting the rule
   * @throws Exception 
   */
  private void handleRule() throws Exception {
    final TreeRule rule;
    if (query.hasContent()) {
      rule = query.serializer().parseTreeRuleV1();
    } else {
      rule = parseRule();
    }
    
    // no matter what, we'll need a tree to work with, so make sure it exists
    final Tree tree = Tree.fetchTree(tsdb, rule.getTreeId())
      .joinUninterruptibly();
    if (tree == null) {
      throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
          "Unable to locate tree: " + rule.getTreeId());
    }
    
    // if get, then we're just returning a rule from a tree
    if (method == HttpMethod.GET) {
      final TreeRule tree_rule = tree.getRule(rule.getLevel(), rule.getOrder());
      if (tree_rule == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate rule: " + rule);
      }
      query.sendReply(query.serializer().formatTreeRuleV1(tree_rule));
    } else if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      try {
        if (rule.storeRule(tsdb, (method == HttpMethod.PUT))
            .joinUninterruptibly()) {
          final TreeRule stored_rule = TreeRule.fetchRule(tsdb, 
              rule.getTreeId(), rule.getLevel(), rule.getOrder())
              .joinUninterruptibly();
          query.sendReply(query.serializer().formatTreeRuleV1(stored_rule));
        } else {
          throw new RuntimeException("Unable to save rule " + rule + 
              " to storage");
        }
      } catch (IllegalStateException e) {
        query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (method == HttpMethod.DELETE) {
      try {
        TreeRule.deleteRule(tsdb, tree.getTreeId(), rule.getLevel(), 
            rule.getOrder()); 
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
        "Unsupported HTTP request method");
    }
  }
  
  private void handleRules() {
    int tree_id = 0;
    List<TreeRule> rules = null;
    if (query.hasContent()) {
      rules = query.serializer().parseTreeRulesV1();
      if (rules == null || rules.isEmpty()) {
        throw new BadRequestException("Missing tree rules");
      }
      
      // validate that they all belong to the same tree
      tree_id = rules.get(0).getTreeId();
      for (TreeRule rule : rules) {
        if (rule.getTreeId() != tree_id) {
          throw new BadRequestException("All rules must belong to the same tree");
        }
      }
    } else {
      tree_id = parseTreeId(false);
    }
    
    // make sure the tree exists
    try {
      if (Tree.fetchTree(tsdb, tree_id).joinUninterruptibly() == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree_id);
      }
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
    
    if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      TreeRule.deleteAllRules(tsdb, tree_id);
      try {
        for (TreeRule rule : rules) {
          rule.storeRule(tsdb, true);
        }
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else if (method == HttpMethod.DELETE) {
      try {
        TreeRule.deleteAllRules(tsdb, tree_id);
        query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
        "Unsupported HTTP request method");
    }
  }
  
  private void handleTest() throws Exception {
    final Map<String, Object> map;
    if (query.hasContent()) {
      map = query.serializer().parseTreeTSUIDsListV1();
    } else {
      map = parseTSUIDsList();
    }
    
    final Integer tree_id = (Integer) map.get("treeId");
    if (tree_id == null) {
      throw new BadRequestException("Missing or invalid Tree ID");
    }
    
    // make sure the tree exists
    final Tree tree = Tree.fetchTree(tsdb, tree_id).joinUninterruptibly();
    if (tree == null) {
      throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
          "Unable to locate tree: " + tree_id);
    }
    
    // ugly, but keeps from having to create a dedicated class just to 
    // convert one field.
    @SuppressWarnings("unchecked")
    final List<String> tsuids = (List<String>)map.get("tsuids");
    if (tsuids == null || tsuids.isEmpty()) {
      throw new BadRequestException("Missing or empty TSUID list");
    }
    
    if (method == HttpMethod.GET || method == HttpMethod.POST || 
        method == HttpMethod.PUT) {
      try {
        final HashMap<String, HashMap<String, Object>> results = 
          new HashMap<String, HashMap<String, Object>>(tsuids.size());
        final TreeBuilder builder = new TreeBuilder(tsdb);
        builder.setTree(tree);
        for (String tsuid : tsuids) {
          final HashMap<String, Object> tsuid_results = 
            new HashMap<String, Object>();
          
          final TSMeta meta = TSMeta.getTSMeta(tsdb, tsuid);
          // if the meta doesn't exist, we can't process, so just log a message
          // to the results and move on to the next TSUID
          if (meta == null) {
            tsuid_results.put("tree", null);
            tsuid_results.put("meta", null);
            final ArrayList<String> messages = new ArrayList<String>(1);
            messages.add("Unable to locate TSUID meta data");
            tsuid_results.put("messages", messages);
            results.put(tsuid, tsuid_results);
            continue;
          }
          
          builder.processTimeseriesMeta(tree_id, meta, true);
          tsuid_results.put("tree", builder.getRootBranch());
          tsuid_results.put("meta", meta);
          tsuid_results.put("messages", builder.getTestMessage());
          
          results.put(tsuid, tsuid_results);
        }
        
        query.sendReply(query.serializer().formatTreeTestV1(results));
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
      "Unsupported HTTP request method");
    }
  }
  
  private void handleCollisionNotMatched(final boolean for_collisions) {
    final Map<String, Object> map;
    if (query.hasContent()) {
      map = query.serializer().parseTreeTSUIDsListV1();
    } else {
      map = parseTSUIDsList();
    }
    
    final Integer tree_id = (Integer) map.get("treeId");
    if (tree_id == null) {
      throw new BadRequestException("Missing or invalid Tree ID");
    }
    
    // make sure the tree exists
    try {
      if (Tree.fetchTree(tsdb, tree_id).joinUninterruptibly() == null) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Unable to locate tree: " + tree_id);
      }
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
    
    if (method == HttpMethod.GET || method == HttpMethod.POST || 
        method == HttpMethod.PUT) {
      try {
        // ugly, but keeps from having to create a dedicated class just to 
        // convert one field.
        @SuppressWarnings("unchecked")
        final List<String> tsuids = (List<String>)map.get("tsuids");
        final Map<String, String> results = for_collisions ? 
            Tree.fetchCollisions(tsdb, tree_id, tsuids) :
              Tree.fetchNotMatched(tsdb, tree_id, tsuids);
        query.sendReply(query.serializer().formatTreeCollisionNotMatchedV1(
            results, for_collisions));
      } catch (ClassCastException e) {
        throw new BadRequestException(
            "Unable to convert the given data to a list", e);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
      "Unsupported HTTP request method");
    }
  }
  
  /**
   * Parses query string parameters into a blank tree object. Used for updating
   * tree meta data.
   * @return A tree object filled in with changes
   * @throws BadRequestException if some of the data was invalid
   */
  private Tree parseTree() {
    final Tree tree = new Tree(parseTreeId(false));
    if (query.hasQueryStringParam("name")) {
      tree.setName(query.getQueryStringParam("name"));
    }
    if (query.hasQueryStringParam("description")) {
      tree.setDescription(query.getQueryStringParam("description"));
    }
    if (query.hasQueryStringParam("notes")) {
      tree.setNotes(query.getQueryStringParam("notes"));
    }
    if (query.hasQueryStringParam("strict_match")) {
      if (query.getQueryStringParam("strict_match").toLowerCase()
          .equals("true")) {
        tree.setStrictMatch(true);
      } else {
        tree.setStrictMatch(false);
      }
    }
    return tree;
  }

  /**
   * Parses query string parameters into a blank tree rule object. Used for 
   * updating individual rules
   * @return A rule object filled in with changes
   * @throws BadRequestException if some of the data was invalid
   */
  private TreeRule parseRule() {
    final TreeRule rule = new TreeRule(parseTreeId(true));
    
    if (query.hasQueryStringParam("type")) {
      try {
        rule.setType(TreeRule.stringToType(query.getQueryStringParam("type")));
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unable to parse the 'type' parameter", e);
      }
    }
    if (query.hasQueryStringParam("field")) {
      rule.setField(query.getQueryStringParam("field"));
    }
    if (query.hasQueryStringParam("custom_field")) {
      rule.setCustomField(query.getQueryStringParam("custom_field"));
    }
    if (query.hasQueryStringParam("regex")) {
      try {
        rule.setRegex(query.getQueryStringParam("regex"));
      } catch (PatternSyntaxException e) {
        throw new BadRequestException(
            "Unable to parse the 'regex' parameter", e);
      }
    }
    if (query.hasQueryStringParam("separator")) {
      rule.setSeparator(query.getQueryStringParam("separator"));
    }
    if (query.hasQueryStringParam("description")) {
      rule.setDescription(query.getQueryStringParam("description"));
    }
    if (query.hasQueryStringParam("notes")) {
      rule.setNotes(query.getQueryStringParam("notes"));
    }
    if (query.hasQueryStringParam("regex_group_idx")) {
      try {
        rule.setRegexGroupIdx(Integer.parseInt(
            query.getQueryStringParam("regex_group_idx")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'regex_group_idx' parameter", e);
      }
    }
    if (query.hasQueryStringParam("display_format")) {
      rule.setDisplayFormat(query.getQueryStringParam("display_format"));
    }
    if (query.hasQueryStringParam("level")) {
      try {
        rule.setLevel(Integer.parseInt(query.getQueryStringParam("level")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'level' parameter", e);
      }
    }
    if (query.hasQueryStringParam("order")) {
      try {
        rule.setOrder(Integer.parseInt(query.getQueryStringParam("order")));
      } catch (NumberFormatException e) {
        throw new BadRequestException(
            "Unable to parse the 'order' parameter", e);
      }
    }
    return rule;
  }
  
  /**
   * Parses the tree ID from a query
   * Used often so it's been broken into it's own method
   * @param required Whether or not the ID is required for the given call
   * @return The tree ID or 0 if not provided
   */
  private int parseTreeId(final boolean required) {
    try{
      if (required) {
        return Integer.parseInt(query.getRequiredQueryStringParam("tree"));
      } else {
        if (query.hasQueryStringParam("tree")) {
          return Integer.parseInt(query.getQueryStringParam("tree"));
        } else {
          return 0;
        }
      }
    } catch (NumberFormatException nfe) {
      throw new BadRequestException("Unable to parse 'tree' value", nfe);
    }
  }

  private Map<String, Object> parseTSUIDsList() {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("treeId", parseTreeId(true));
    
    final String tsquery = query.getQueryStringParam("tsuids");
    if (tsquery != null) {
      final String[] tsuids = tsquery.split(",");
      map.put("tsuids", Arrays.asList(tsuids));
    }
    
    return map;
  }
}

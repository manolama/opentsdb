package net.opentsdb.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.GeneralMeta.Meta_Type;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.JSON;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is responsible for creating and maintaining a tree of 
 * timeseries data for easy navigation
 */
//@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonAutoDetect(fieldVisibility = Visibility.PUBLIC_ONLY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaTree {
  @JsonIgnore
  private static final Logger LOG = LoggerFactory.getLogger(MetaTree.class);
  
  /**
   * Shortcut for dealing with the type of rule
   */
  public enum Tree_Rule_Type{
    METRIC,
    METRIC_CUSTOM,
    TAGK,
    TAGK_CUSTOM,
    TAGV_CUSTOM,
  }
  
  /* The name of this particular tree */
  private String name;
  /* Notes about this tree */
  private String notes;
  /* Whether or not to put non-matching branches/leaves in the tree */
  private boolean strict_match;
  
  private boolean c_name;
  private boolean c_notes;
  private boolean c_strict_match;
  
  /* ID of the tree */
  private int tree_id;
  /* Sorted, two layer map of rules to use in building the tree */  
  private TreeMap<Integer, TreeMap<Integer, MetaTreeRule>> rules;
  /* Tracks the max level index for times when users forget a level */
  private int max_rule_level;
  /* A list of non-matched timeseries UIDs that were not included in the tree */
  private HashSet<String> no_matches;
  /* A list of TSUID collissions that wouldn't make it onto the tree */
  private HashSet<String> collissions;
  /* Total number of branches in the tree */
  private int num_branches;
  /* Total number of leaves in the tree */
  private int num_leaves;
  /* Epoch timestamp when this tree was created */
  private long created;
  /* Epoch timestamp when a full rebuild of this tree was started */
  private long full_sync_start;
  /* Epoch timestamp when a full rebuild of this tree was completed. It's 0 until completed */
  private long full_sync_completed;
  /* Epoch timestamp when the tree was last updated, e.g. via a new TSUID in-between rebuilds */
  private long last_update;
  /* Current version of the tree, used to cull stale branches and leaves */
  private long version;
  /* Maximum depth of the tree */
  private int max_depth;
  /* The limit before flushing the temp hashes to disk */
  private int flush_limit;
  
  
  @JsonIgnore
  private String tree_separator = "|";
  
  /*these be the temp branches that are loaded, then flushed to "storage" */
  @JsonIgnore
  private HashMap<Integer, MetaTreeBranch> temp_branches = new HashMap<Integer, MetaTreeBranch>();
  
  @JsonIgnore
  private ArrayList<String> parsing_messages = new ArrayList<String>();
  
  @JsonIgnore
  private boolean load_meta = false;
  
  @JsonIgnore
  //private TsdbStore storage;
  
  public MetaTree(final int tree_id){
    this.tree_id = tree_id;
    this.rules = new TreeMap<Integer, TreeMap<Integer, MetaTreeRule>>();
    this.no_matches = new HashSet<String>();
    this.collissions = new HashSet<String>();
    this.max_rule_level = 0;
    this.flush_limit = 500;
  }
  
  public MetaTree(){
    this.tree_id = 0;
    this.rules = new TreeMap<Integer, TreeMap<Integer, MetaTreeRule>>();
    this.no_matches = new HashSet<String>();
    this.collissions = new HashSet<String>();
    this.max_rule_level = 0;
    this.flush_limit = 500;
  }
  
  public void copyChanges(final MetaTree tree){
    if (tree.c_name)
      this.name = tree.name;
    if (tree.c_notes)
      this.notes = tree.notes;
    if (tree.c_strict_match)
      this.strict_match = tree.strict_match;
  }
  
  /**
   * Adds or replaces the rule in the proper spot in the rule tree
   * @param rule The rule to add
   * @returnTrue
   */
  public void AddRule(final MetaTreeRule rule){
    TreeMap<Integer, MetaTreeRule> r = this.rules.get(rule.getLevel());
    if (r == null){
      r = new TreeMap<Integer, MetaTreeRule>();
      r.put(rule.getOrder(), rule);
      this.rules.put(rule.getLevel(), r);
    }else{
      r.put(rule.getOrder(), rule);
    }
    
    if (rule.getLevel() > this.max_rule_level)
      this.max_rule_level = rule.getLevel();
  }

  /**
   * Walks the TSUID table, loading the metadata and builds the tree
   */
  public void Reindex(final TSDB tsdb){
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    scanner.setFamily(TsdbStore.toBytes("name"));
    
    this.version++;
    this.full_sync_start = System.currentTimeMillis()/1000;
    this.full_sync_completed = 0;
    this.StoreTree(tsdb.uid_storage);
    
    long start = this.full_sync_start;
    this.setLoadMeta();
    
    // put new root
    MetaTreeBranch root;
    try {
      root = this.FetchBranch(tsdb.uid_storage, 0);
      if (root == null){
        root = new MetaTreeBranch(0);
        root.tree_id = this.tree_id;
        root.display_name = "ROOT";
      }
      root.tree_version = this.version;
      this.StoreBranch(tsdb.uid_storage, root);
    } catch (JsonParseException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (JsonMappingException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    this.CalculateMaxLevel();
    
    // if we don't have rules, there's nothing to process, duh!
    if (rules.size() < 1)
      return;
    
    try {
      scanner = tsdb.uid_storage.openScanner(scanner);

      long count=0;
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = tsdb.uid_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            if (TsdbStore.fromBytes(cell.qualifier()).compareTo("ts_meta") == 0){
              final String uid = UniqueId.IDtoString(cell.key());
              
              if (cell.value() == null){
                LOG.warn(String.format("Metadata was null [%s]", uid));
                continue;
              }
              
              TimeSeriesMeta meta = (TimeSeriesMeta)JSON.parseToObject(cell.value(), TimeSeriesMeta.class);
              if (meta == null){
                LOG.warn(String.format("Error retrieving TSUID metadata [%s]", uid));
                continue;
              }
              
              // load the general metas
              byte[] metricID = MetaDataCache.getMetricID(cell.key());
              if (this.load_meta){
                GeneralMeta m = tsdb.metrics.getGeneralMeta(metricID);
                meta.setMetric(m);
              }else{
                GeneralMeta m = new GeneralMeta(metricID);
                try{
                  m.setName(tsdb.metrics.getName(metricID));
                }catch (Exception e){
                  LOG.error(e.getMessage());
                  continue;
                }
                meta.setMetric(m);
              }
              
              ArrayList<byte[]> tags = MetaDataCache.getTagIDs(cell.key());
              ArrayList<GeneralMeta> tag_metas = new ArrayList<GeneralMeta>();
              int index=1;
              for (byte[] tag : tags){
                if ((index % 2) != 0){
                  GeneralMeta tagk;
                  if (this.load_meta){
                    tagk = tsdb.tag_names.getGeneralMeta(tag);
                    if (tagk == null){
                      LOG.warn(String.format("Unable to get tagk value for [%s]", UniqueId.IDtoString(tag)));
                      break;
                    }
                  }else{
                    tagk = new GeneralMeta(tag);
                    tagk.setType(Meta_Type.TAGK);
                    try{
                      tagk.setName(tsdb.tag_names.getName(tag));
                    }catch (Exception e){
                      LOG.error(e.getMessage());
                      continue;
                    }
                  }
                  tag_metas.add(tagk);
                }else{
                  GeneralMeta tagv;
                  if (this.load_meta){
                    tagv = tsdb.tag_values.getGeneralMeta(tag);
                    if (tagv == null){
                      LOG.warn(String.format("Unable to get tagv value for [%s]", UniqueId.IDtoString(tag)));
                      break;
                    }
                  }else{
                    tagv = new GeneralMeta(tag);
                    tagv.setType(Meta_Type.TAGV);
                    try{
                      tagv.setName(tsdb.tag_values.getName(tag));
                    }catch (Exception e){
                      LOG.error(e.getMessage());
                      continue;
                    }
                  }
                  tag_metas.add(tagv);
                }
                index++;
              }
              if (tag_metas.size() % 2 != 0){
                LOG.warn(String.format("Improper number of tags detected: [%d]", tag_metas.size()));
                continue;
              }
              
              meta.setTags(tag_metas);
              
              this.ProcessTS(tsdb.uid_storage, meta);
              count++;
              
              if (this.temp_branches.size() > flush_limit)
                this.FlushTemp(tsdb.uid_storage);
              
              if (count % 1000 == 0){
                LOG.info(String.format("Processed [%d] meta data entries in tree in [%d]s", 
                    count, ((System.currentTimeMillis() / 1000) - start)));
                start = System.currentTimeMillis() / 1000;                
              }
            }
          }
        }
      }
      
      if (this.temp_branches.size() > 0)
        this.FlushTemp(tsdb.uid_storage);
      
      LOG.info(String.format("Indexed [%d] TSUID metadata into tree id [%d]", count, this.tree_id));
      
      root = this.FetchBranch(tsdb.uid_storage, 0);
      if (root == null){
        LOG.error(String.format("Unable to find root branch for tree [%d]", this.tree_id));
        return;
      }
      this.UpdateCounts(tsdb.uid_storage, root);
      this.full_sync_completed = System.currentTimeMillis()/1000;
      this.StoreTree(tsdb.uid_storage);
      return;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
  /**
   * Tests a tree's ruleset on a timeseries ID to see what would happen
   * @param tsuid
   * @return
   * @throws IOException 
   * @throws JsonMappingException 
   * @throws JsonParseException 
   */
  public String TestTS(final TSDB tsdb, final String tsuid, final boolean save) 
    throws JsonParseException, JsonMappingException, IOException{
    TimeSeriesMeta meta = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(tsuid));
    if (meta == null){
      LOG.debug(String.format("Unable to locate TSUID [%s]", tsuid));
      return "{\"error\":\"Could not find an entry for the given TSUID\"}";
    }
    
    this.CalculateMaxLevel();
    
    boolean[] had_nomatch = { false };
    MetaTreeBranch br = ProcessTS(meta, null, 0, new TreeRecursionState(), had_nomatch);
    if (br == null){
      LOG.error("Branch returned was null");
      return "{\"error\":\"Branch returned was null\"}";
    }
    if (had_nomatch[0] && this.strict_match)
      this.parsing_messages.add("Detected a no match on the tree and it would not be added");

    HashMap<String, Object> tree = new HashMap<String, Object>();
    tree.put("timeseries", meta);
    if (!AddBranchToTree(tsdb.uid_storage, br, true))
      tree.put("error", "Unable to add TSUID to the tree");    
    tree.put("parse_log", this.parsing_messages);
    this.TestPrint(this.temp_branches.get(0), tree);
    
    if (save)
      this.FlushTemp(tsdb.uid_storage);
    
    return JSON.serializeToString(tree);
  }
  
  /**
   * Adds the timeseries to the tree
   * @param ts The timeseries metadata to process
   * @return True if processed successfully, false if there was an error or the
   * timeseries didn't match any of the rules and NMs are disabled for the tree
   * @throws IOException 
   * @throws JsonMappingException 
   * @throws JsonParseException 
   */
  public boolean ProcessTS(final TsdbStore storage, final TimeSeriesMeta ts) 
    throws JsonParseException, JsonMappingException, IOException{
    if (ts.getUID().isEmpty()){
      LOG.warn("Encountered an emtpy TS UID");
      return false;
    }
    
    LOG.trace("Processing TS: " + ts.toString());
    
    this.parsing_messages.clear();
    
    boolean[] had_nomatch = { false };
    MetaTreeBranch br = ProcessTS(ts, null, 0, new TreeRecursionState(), had_nomatch);
    if (this.strict_match && had_nomatch[0]){
      LOG.trace("Branch had a no match and NMs are disabled, won't add to tree: " + br);
      this.no_matches.add(ts.getUID());
      return false;
    }else if (br == null){
      LOG.warn("Branch returned was null");
      return false;
    }else{
      // now we can loop through the tree and build the hash
      AddBranchToTree(storage, br, false);
      return true;
    }
  }
  
  public void copy(final MetaTree tree){
    this.name = tree.name;
    this.notes = tree.notes;
    this.strict_match = tree.strict_match;
    
    this.tree_id = tree.tree_id;
    this.rules = tree.rules;
    this.no_matches = tree.no_matches;
    this.collissions = tree.collissions;
    this.num_branches = tree.num_branches;
    this.num_leaves = tree.num_leaves;
    this.created = tree.created;
    this.full_sync_start = tree.full_sync_start;
    this.full_sync_completed = tree.full_sync_completed;
    this.last_update = tree.last_update;
    this.version = tree.version;
    this.max_depth = tree.max_depth;
    
    this.tree_separator = tree.tree_separator;
    this.temp_branches = tree.temp_branches;
    this.load_meta = tree.load_meta;
  }
  
  /**
   * Scans the tree config row for an unused ID
   * If no trees have been configured, it will return a 1
   * @return an ID greater than 0 if found, 0 if not found
   */
  public static int GetNewID(final TsdbStore storage){
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    scanner.setFamily(TsdbStore.toBytes("name"));
    scanner.setStartRow(new byte[] { 0x01, 0x00} );
    scanner.setEndRow(new byte[] { 0x01, 0x00} );
    try {
      scanner = storage.openScanner(scanner);
      byte[] tree_id = null;
      
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            tree_id = cell.qualifier();
          }
        }
      }
      
      // if null, we don't have any tree IDs! So return 1
      if (tree_id == null){
        return 1;
      }
      
      // convert
      byte[] four_byte = { 0x00, 0x00, 0x00, tree_id[0]};
      int id = Bytes.getInt(four_byte);
      if (id >= 254){
        LOG.error("Reached the maximum number of trees in the configuration row");
        return 0;
      }
      id++;
      return id;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }

  public final MetaTreeBranchDisplay GetBranch(final TsdbStore storage, int branch) 
    throws JsonParseException, JsonMappingException, IOException{
    MetaTreeBranch b = this.FetchBranch(storage, branch);
    if (b == null)
      return null;
    return new MetaTreeBranchDisplay(b);
  }
  
  public final MetaTreeRule GetRule(final int level, final int order){
    TreeMap<Integer, MetaTreeRule> ruleset = this.rules.get(level);
    if (ruleset == null)
      return null;
    
    return ruleset.get(order);
  }
  
  public final String DeleteRule(final int level, final int order, final boolean entire_level){
    if (!this.rules.containsKey(level))
      return "Rule level did not exist";
    
    if (entire_level){
      this.rules.remove(level);
      LOG.info(String.format("Removed entire rule level [%d] for tree [%d]", level, this.tree_id));
      return "";
    }
    
    TreeMap<Integer, MetaTreeRule> ruleset = this.rules.get(level);
    if (!ruleset.containsKey(order))
      return "Rule at the given level and order did not exist";
    
    ruleset.remove(order);
    LOG.info(String.format("Removed rule [%d:%d] from tree [%d]", level, order, this.tree_id));
    if (ruleset.size() < 1){
      this.rules.remove(level);
      LOG.info(String.format("Level [%d] was empty, removed level from tree [%d]", level, this.tree_id));
    }
    return "";
  }
  
  public static final ArrayList<MetaTree> GetTrees(final TsdbStore storage){
    TsdbScanner scanner = new TsdbScanner(null, null, TsdbStore.toBytes("tsdb-uid"));
    scanner.setFamily(TsdbStore.toBytes("name"));
    scanner.setStartRow(new byte[] { 0x01, 0x00} );
    scanner.setEndRow(new byte[] { 0x01, 0x00} );
    try {
      scanner = storage.openScanner(scanner);
      ArrayList<MetaTree> trees = new ArrayList<MetaTree>();
      MetaTree tree = new MetaTree();  
      
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          for (KeyValue cell : row){
            tree = (MetaTree)JSON.parseToObject(cell.value(), MetaTree.class);
            trees.add(tree);
          }
        }
      }
      
      return trees;
    } catch (HBaseException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Should never be here", e);
    }
  }
  
/* PRIVATE METHODS ----------------------------------- */   
  
  /**
   * Recursive function that processes a timeseries metadata and splits it into branches and leaves
   * @param ts The timeseries to process
   * @param branch The current branch level, may be root
   * @param depth The current depth of the branch
   * @param state The current state of recursion, should be NULL the first time through
   * @param no_match A return variable that tells the caller whether a NO MATCH was dected
   * somewhere in the branch
   * @return A processed tree representing the timeseries meta. Needs to be synchronized
   * with the main tree.
   */
  private MetaTreeBranch ProcessTS(final TimeSeriesMeta ts, MetaTreeBranch branch, int depth, 
      TreeRecursionState state, boolean[] no_match){    
    // make sure we haven't exceeded the rule limit
    if (state.rule_idx > this.max_rule_level){
      LOG.trace("Reached the final rule in the list");
      return null;
    }
    
    // setup the local branch
    MetaTreeBranch current_branch = new MetaTreeBranch(branch != null ? branch.name.hashCode() : 0);
    current_branch.depth = depth;
    state.current_branch = current_branch;
    String message;
    String idx = "";
    
    // get the current rule level
    TreeMap<Integer, MetaTreeRule> current_rules = null;
    while (current_rules == null && state.rule_idx <= this.max_rule_level){
      current_rules = this.rules.get(state.rule_idx);
      if (current_rules == null)
        state.rule_idx++;
    }
    if (current_rules == null){
      System.out.println("Ran out of rules [" + state.rule_idx + "]");
      return null;
    }
    
    int count=0;
    for (Map.Entry<Integer, MetaTreeRule> entry : current_rules.entrySet()){
      MetaTreeRule rule = entry.getValue();
      idx = "[" + rule.getLevel() + ":" + rule.getOrder() +"]";
      message = String.format("Processing rule [%s]", idx);
      this.parsing_messages.add(message);
      LOG.trace(message);
      if (rule.ruleType() == Tree_Rule_Type.METRIC){
        state = ProcessRule(state, branch, rule, ts.getMetric().getName(), ts);
        current_branch = state.current_branch;
      }else if (rule.ruleType() == Tree_Rule_Type.TAGK){
        // TAGK HANDLER
        String tag = "";
        boolean found = false;
        for (GeneralMeta gm : ts.getTags()){
          if (gm.getType() == Meta_Type.TAGK && gm.getName().compareTo(rule.getField()) == 0)
            found = true;
          if (gm.getType() == Meta_Type.TAGV && found){
            tag = gm.getName();
            break;
          }
        }
        if (found && !tag.isEmpty()){
          message = String.format(idx + " Matched tagk [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
          state = ProcessRule(state, branch, rule, tag, ts);
          current_branch = state.current_branch;
        }else{
          message = String.format(idx + " No match on tagk [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
        }   
      }else if (rule.ruleType() == Tree_Rule_Type.METRIC_CUSTOM){
        Map<String, String> custom = ts.getMetric().getCustom();
        if (custom != null && custom.containsKey(rule.getField())){
          message = String.format(idx + " Matched metric custom tag [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
          state = ProcessRule(state, branch, rule, custom.get(rule.getField()), ts);
          current_branch = state.current_branch;
        }else{
          message = String.format(idx + " No match on custom tag [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
        } 
      }else if (rule.ruleType() == Tree_Rule_Type.TAGK_CUSTOM){
        GeneralMeta tagk = null;
        for (GeneralMeta gm : ts.getTags()){
          if (gm.getType() == Meta_Type.TAGK && gm.getName().compareTo(rule.getField()) == 0){
            tagk = gm;
            break;
          }
        }
        if (tagk != null){
          message = String.format(idx + " Matched tagk [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
          Map<String, String> custom = tagk.getCustom();
          if (custom != null && custom.containsKey(rule.getCustom_field())){
            message = String.format(idx + " Matched tagk custom tag [%s]", rule.getCustom_field());
            this.parsing_messages.add(message);
            LOG.trace(message);
            state = ProcessRule(state, branch, rule, custom.get(rule.getCustom_field()), ts);
            current_branch = state.current_branch;
          }else{
            message = String.format(idx + " No match on tagk custom tag [%s]", rule.getCustom_field());
            this.parsing_messages.add(message);
            LOG.trace(message);
          } 
        }else{
          message = String.format(idx + " No match on tagk [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
        }   
      }else if (rule.ruleType() == Tree_Rule_Type.TAGV_CUSTOM){
        GeneralMeta tagv = null;
        for (GeneralMeta gm : ts.getTags()){
          if (gm.getType() == Meta_Type.TAGV && gm.getName().compareTo(rule.getField()) == 0){
            tagv = gm;
            break;
          }
        }
        if (tagv != null){
          message = String.format(idx + " Matched tagv [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
          Map<String, String> custom = tagv.getCustom();
          if (custom != null && custom.containsKey(rule.getCustom_field())){
            message = String.format(idx + " Matched tagv custom tag [%s]", rule.getCustom_field());
            this.parsing_messages.add(message);
            LOG.trace(message);
            state = ProcessRule(state, branch, rule, custom.get(rule.getCustom_field()), ts);
            current_branch = state.current_branch;
          }else{
            message = String.format(idx + " No match on tagv custom tag [%s]", rule.getCustom_field());
            this.parsing_messages.add(message);
            LOG.trace(message);
          } 
        }else{
          message = String.format(idx + " No match on tagv [%s]", rule.getField());
          this.parsing_messages.add(message);
          LOG.trace(message);
        }   
      }else{
        message = idx + " Rule type is not currently supported: " + rule;
        this.parsing_messages.add(message);
        LOG.warn(message);
      }
      
      // if we have successfully matched a rule out of multiple in the level, we need to bail
      if (!current_branch.name.isEmpty()){
        message = String.format(idx + " Matched name [%s] on sub rule: %s", current_branch.name, rule);
        this.parsing_messages.add(message);
        LOG.trace(message);
        break;
      }
      count++;
    }
    
    // if no match was detected, we need to set the flag to bubble up
    if (current_branch.name.isEmpty())
      no_match[0] = true;
    
    // determine if we need to increment the rule index or keep processing splits
    if (state.splits != null && state.split_idx >= state.splits.length){
      //LOG.trace("No more splits detected, resetting");
      state.splits = null;
      state.split_idx = 0;
      state.rule_idx++;
    }else if (state.splits != null){
      // keep going
    }else
      state.rule_idx++;
    
    // recurse again till we hit a leaf or run out of rules
    MetaTreeBranch r_branch = ProcessTS(ts, current_branch, ++depth, state, no_match);
    
    // now we decide what to do with our results
    if (r_branch == null || (r_branch.name.isEmpty() &&
        r_branch.branch == null && r_branch.leaf == null)){
      // this means we reached the end of the recursion
      if (current_branch.name.isEmpty())
        return current_branch;
      
      // we have a leaf! Make a leaf and add it to parent
      MetaTreeLeaf leaf = new MetaTreeLeaf(current_branch, ts);
      if (branch == null){
        LOG.error(String.format(idx + " Attempting to add a leaf to a NULL parent branch: %s", state));
      }else{
        branch.leaf = leaf;
        message = String.format(idx + " Adding leaf [%s] to parent branch [%s]", leaf.name, branch);
        this.parsing_messages.add(message);
        LOG.trace(message);
      }
    }else if (current_branch.name.isEmpty() && 
        !r_branch.name.isEmpty()){
      message = String.format(idx + " Skipping a no match branch, returning [%s]", r_branch);
      this.parsing_messages.add(message);
      LOG.trace(message);
      return r_branch;
    }else{
      if (branch != null){
        branch.branch = current_branch;
        message = String.format(idx + " Adding branch [%s] to parent branch [%s]", current_branch, branch);
        this.parsing_messages.add(message);
        LOG.trace(message);
      }else
        LOG.trace(String.format(idx + " Found root branch [%s]", current_branch));
    }
    
    return current_branch;
  }
  
  /**
   * Processes a given rule on the given value parsed by the recursive function
   * @param state The current recursion state
   * @param branch Current branch we're processing on
   * @param rule Current rule we're working with
   * @param value Value extracted by the rule
   * @param ts Timeseries metadata
   * @return The state after processing
   */
  private TreeRecursionState ProcessRule(TreeRecursionState state, 
      MetaTreeBranch branch, MetaTreeRule rule, String value, TimeSeriesMeta ts){
    if (rule.getRe() == null &&
        (rule.getSeparator() == null || rule.getSeparator().isEmpty())){
      state.current_branch.name = (branch != null && !branch.name.isEmpty() ? branch.name + tree_separator + value : value);     
      state.current_branch.display_name = ProcessDisplayFormatter(rule, ts, value, value);
    }else if(rule.getRe() != null){
      state = ProcessRegex(state, branch, rule, value, ts);
    }else if (rule.getSeparator() != null && !rule.getSeparator().isEmpty()){
      state = Process_Split(state, branch, rule, value, ts);
    }else{
      LOG.error("Unknown rule processing state: " + state);
      this.parsing_messages.add("Unknown rule processing state: " + state);
    }
    return state;
  }
  
  /**
   * Handles splitting or processing a split value
   * @param state The current recursion state
   * @param branch Current branch we're processing on
   * @param rule Current rule we're working with
   * @param value Value extracted by the rule
   * @param ts Timeseries metadata
   * @return The state after processing
   */
  private TreeRecursionState Process_Split(TreeRecursionState state, 
      MetaTreeBranch branch, MetaTreeRule rule, String value, TimeSeriesMeta ts){
    try{
      if(state.splits == null){
        state.splits = value.split(rule.getSeparator());
        state.current_branch.name = (branch != null && !branch.name.isEmpty() ? branch.name + tree_separator + 
            state.splits[state.split_idx] : state.splits[state.split_idx]);
        state.current_branch.display_name = ProcessDisplayFormatter(rule, ts, value, 
            state.splits[state.split_idx]);
        state.split_idx++;
      }else{
        state.current_branch.name = (branch != null && !branch.name.isEmpty() ? branch.name + tree_separator + 
            state.splits[state.split_idx] : state.splits[state.split_idx]);
        state.current_branch.display_name = ProcessDisplayFormatter(rule, ts, value, 
            state.splits[state.split_idx]);
        state.split_idx++;
      }
    }catch (Exception e){
      LOG.error(e.getMessage());
    }
    return state;
  }
  
  /**
   * Attempts to match a regular expresion on the value and extract a token
   * @param state The current recursion state
   * @param branch Current branch we're processing on
   * @param rule Current rule we're working with
   * @param value Value extracted by the rule
   * @param ts Timeseries metadata
   * @return The state after processing
   */
  private TreeRecursionState ProcessRegex(TreeRecursionState state, 
      MetaTreeBranch branch, MetaTreeRule rule, String value, TimeSeriesMeta ts){
    try{
      String message;
      Matcher m = rule.getRe().matcher(value);
      if (m.find()){
        if (m.groupCount() >= rule.getRegex_group_idx() + 1){
          final String extracted = m.group(rule.getRegex_group_idx() + 1);
          if (extracted == null || extracted.isEmpty()){
            message = String.format("Extracted value for rule [%d:%d] was null or empty", 
                rule.getLevel(), rule.getOrder());
            this.parsing_messages.add(message);
            LOG.warn(message);
            System.out.println("Warning: Extracted value was null or empty");
          }else{
            state.current_branch.name = (branch != null && !branch.name.isEmpty() ? branch.name + tree_separator + 
                extracted : extracted);
            state.current_branch.display_name = ProcessDisplayFormatter(rule, ts, value, 
                extracted);
          }
        }else{
          message = String.format("Regex group index [%d] for rule [%d:%d] was out of range [%d]", 
              rule.getRegex_group_idx(), rule.getLevel(), rule.getOrder(), m.groupCount());
          this.parsing_messages.add(message);
          LOG.warn(message);
        }
      }
    }catch (Exception e){
      LOG.error(e.getMessage());
    }
    return state;
  }
  
  /**
   * Formats the display_name according to the rule's display formatter
   * If the display_format field is empty or null, the given value will be returned
   * immediately
   * @param rule Rule to use when processing the formatter
   * @param ts Original timeseries object
   * @param ov Original value that was processed by this rule
   * @param v Value post processing by the rule, e.g. could be a regex extraction
   * @return A string to be used for display purposes
   */
  private String ProcessDisplayFormatter(MetaTreeRule rule, TimeSeriesMeta ts, String ov, String v){
    if (rule.getDisplay_format() == null || rule.getDisplay_format().isEmpty())
      return v;
    
    String rv = rule.getDisplay_format();
    if (rv.contains("{ovalue}"))
      rv = rv.replace("{ovalue}", ov);

    if (rv.contains("{value}"))
      rv = rv.replace("{value}", v);
    
    if (rv.contains("{tsuid}"))
      rv = rv.replace("{tsuid}", ts.getUID());
      
    if (rv.contains("{tag_name}")){
      if (rule.ruleType() == Tree_Rule_Type.TAGK)
        rv = rv.replace("{tag_name}", rule.getField());
      else if (rule.ruleType() == Tree_Rule_Type.METRIC_CUSTOM ||
          rule.ruleType() == Tree_Rule_Type.TAGK_CUSTOM ||
          rule.ruleType() == Tree_Rule_Type.TAGV_CUSTOM){
        rv = rv.replace("{tag_name}", rule.getCustom_field());
      }else{
        LOG.debug(String.format("Display rule [%d:%d] didn't match a valid source, zeroing {tag_name}", 
            rule.getLevel(), rule.getOrder()));
        rv = rv.replace("{tag_name}", "");
      }
    }
    
    return rv;
  }
  
  /**
   * Updates the counts for the current tree by iterating through every
   * branch in the tree recursively.
   * 
   * I'm sure there is a better way, just haven't gotten to it yet.
   * @param branch The branch to process. If calling the first time, give the root 
   */
  private void UpdateCounts(final TsdbStore storage, MetaTreeBranch branch){
    if (branch.branches == null || branch.branches.size() < 1)
      return;
    
    boolean updated = false;
    for (MetaTreeStoreBranch b : branch.branches){
      MetaTreeBranch cb;
      try {
        cb = FetchBranch(storage, b.branch_id);
        if (cb == null){
          System.out.println("Error fetching branch [" + b.branch_id + "]");
        }else{
          b.num_branches = cb.branches == null ? 0 : cb.branches.size();
          b.num_leaves = cb.leaves == null ? 0 : cb.leaves.size();
          
          this.num_branches += b.num_branches;
          this.num_leaves += b.num_leaves;
          
          if (branch.depth + 1 > this.max_depth)
            this.max_depth = branch.depth + 1;
          updated = true;
        }
      } catch (JsonParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (JsonMappingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }
    if (updated)
      try {
        StoreBranch(storage, branch);
      } catch (JsonParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (JsonMappingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
  }
 
  /**
   * Attempts to retrieve the given branch from cache or storage
   * @param hash The hash to fetch
   * @return A branch if found, null if not
   * @throws IOException 
   * @throws JsonMappingException 
   * @throws JsonParseException 
   */
  private final MetaTreeBranch FetchBranch(final TsdbStore storage, final int hash) 
    throws JsonParseException, JsonMappingException, IOException{
    if (this.tree_id < 1){
      LOG.error("Tree ID was not set");
      return null;
    }
    
    MetaTreeBranch b = temp_branches.get(hash);
    if (b != null)
      return b;
    
    byte[] id = { 0x01, ((Integer)this.tree_id).byteValue()};
    byte[] c = Bytes.fromInt(hash);
    
    final byte[] raw = storage.getValue(id, TsdbStore.toBytes("name"), c);
    final String json = (raw == null ? null : TsdbStore.fromBytes(raw));
    if (json != null){
      return (MetaTreeBranch)JSON.parseToObject(json, MetaTreeBranch.class);
    }    
    LOG.debug(String.format("Unable to locate branch [%d] in storage for tree [%d]", 
        hash, this.tree_id));
    return null;
  }
  
  /**
   * Attempts to store the branch in storage
   * @param branch The branch to store
   * @return The branch after reading from storage, use it as a verification step
   * @throws IOException 
   * @throws JsonMappingException 
   * @throws JsonParseException 
   */
  private MetaTreeBranch StoreBranch(final TsdbStore storage, MetaTreeBranch branch) 
    throws JsonParseException, JsonMappingException, IOException{
    if (this.tree_id < 1){
      LOG.error("Missing Tree ID");
      return null;
    }
    
    byte[] json = JSON.serializeToBytes(branch);
    byte[] id = { 0x01, ((Integer)this.tree_id).byteValue()};
    byte[] c = Bytes.fromInt(branch.hashCode());
    
    try {
      storage.putWithRetry(id, TsdbStore.toBytes("name"), c, json);
      LOG.debug("Updated branch in storage: " + branch);
    } catch (HBaseException e) {
      LOG.error("Failed to Put branch [" + branch + "]: " + e);
      return null;
    }
    
    return FetchBranch(storage, branch.hashCode());
  }
  
  /**
   * Attempts to add a processed branch to the tree
   * Should be called with the results of the ProcessTS() method
   * @param b The TS branch to process into the tree
   * @return True if processed successfully, false if there was an error
   * @throws IOException 
   * @throws JsonMappingException 
   * @throws JsonParseException 
   */
  private boolean AddBranchToTree(final TsdbStore storage, final MetaTreeBranch b, final boolean testing) 
  throws JsonParseException, JsonMappingException, IOException{ 
    String message;
    
    // if we have a parent, add the current branch to it
    MetaTreeBranch parent = FetchBranch(storage, b.parent_hash);
    if (parent == null){
      LOG.warn(String.format("Unable to find the parent branch: %s", b));
      return false;
    }else{
      if (parent.branches == null)
        parent.branches = new TreeSet<MetaTreeStoreBranch>();
      
      MetaTreeStoreBranch mtsb = new MetaTreeStoreBranch(b);
      if (!parent.branches.contains(mtsb)){
        parent.branches.add(mtsb);
        message = "Updated parent with branch: " + b;
        this.parsing_messages.add(message);
        LOG.trace(message);
        this.temp_branches.put(parent.hashCode(), parent);
      }else{
        message = "Parent already has branch: " + b;
        this.parsing_messages.add(message);
        if (testing)
          this.temp_branches.put(parent.hashCode(), parent);
      }
    }
      
    // now check/store the branch
    MetaTreeBranch local = FetchBranch(storage, b.hashCode());
    if (local == null){
      temp_branches.put(b.hashCode(), b);
      message = "Added a new branch to tree: " + b;
      this.parsing_messages.add(message);
      LOG.trace(message);
      local = FetchBranch(storage, b.hashCode());
    }
    
    // if this branch had a leaf, add it to the proper spot
    if (b.leaf != null){
      LOG.trace(String.format("Branch [%d] has [%d] leaves", 
          local.hashCode(), (local.leaves == null ? 0 : local.leaves.size())));
      MetaTreeStoreLeaf leaf = new MetaTreeStoreLeaf(b.leaf);
      if (local.leaves == null)
        local.leaves = new TreeSet<MetaTreeStoreLeaf>();
      if (!local.leaves.contains(leaf)){
       local.leaves.add(leaf);
        message = "Added new leaf to branch: " + b.leaf + "  It now has [" + local.leaves.size() + "] leaves";
        this.parsing_messages.add(message);
        LOG.trace(message);
        this.temp_branches.put(local.hashCode(), local);
      }else{
//         message = "Leaf collision: " + b.leaf;
//        if (testing){
//          for (MetaTreeStoreLeaf l : local.leaves){
//            LOG.debug("Have leaf: " + l.hash + " and new " + leaf.hash);
//            if (l.hashCode() == leaf.hashCode())
//              message = "Leaf collision- Old: " + l + "  New: " + b.leaf;
//          }
//        }
        message = "Leaf already belongs to node: " + b.leaf;
        this.parsing_messages.add(message);
        LOG.trace(message);
        //this.collissions.add(leaf.tsuid);
        if (testing){
          local.leaves.add(leaf);
          this.temp_branches.put(local.hashCode(), local);
        }
        return true;
      }
    }
    
    if (b.branch != null)
      return AddBranchToTree(storage, b.branch, testing);
    else if (b.leaf == null){
      LOG.error("Ran into a null branch without leaves: " + b);
      return false;
    }
    if (testing)
      this.temp_branches.put(local.hashCode(), local);
    return true;
  }
  
  private void setLoadMeta(){
    if (this.rules.size() < 1)
      return;
    
    for (TreeMap<Integer, MetaTreeRule> rule_set : this.rules.values()){
      for (MetaTreeRule rule : rule_set.values()){
        if (rule.getType() == 1 || 
            rule.getType() == 3|| 
            rule.getType() == 4){
          this.load_meta = true;
          return;
        }
      }        
    }
  }
  
  public boolean StoreTree(final TsdbStore storage){
    if (this.tree_id < 1){
      LOG.error("Missing Tree ID");
      return false;
    }
    this.last_update = System.currentTimeMillis() / 1000;
    // fix missing creation values
    if (this.created < 1)
      this.created = this.last_update;
    
    try { 
      byte[] json = JSON.serializeToBytes(this);
      byte[] rowid = { 0x01, 0x00};
      byte[] c = {((Integer)this.tree_id).byteValue()};
      
      storage.putWithRetry(rowid, TsdbStore.toBytes("name"), c, json);
      LOG.debug("Updated tree in storage: " + this);
      return true;
    } catch (HBaseException e) {
      LOG.error("Failed to Put tree [" + this + "]: " + e);
      return false;
    } catch (JsonGenerationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }
  
  public boolean LoadTree(final TsdbStore storage, int id) 
    throws JsonParseException, JsonMappingException, IOException{
    if (id < 1){
      LOG.error("Tree ID was not set");
      return false;
    }
    
    byte[] rowid = { 0x01, 0x00};
    byte[] c = {((Integer)id).byteValue()};
    
    final byte[] raw = storage.getValue(rowid, TsdbStore.toBytes("name"), c);
    final String json = (raw == null ? null : TsdbStore.fromBytes(raw));
    if (json != null){
      MetaTree tree = (MetaTree)JSON.parseToObject(json, MetaTree.class);
      this.copy(tree);
      this.setLoadMeta();
      return true;
    }    
    LOG.debug(String.format("Unable to locate tree [%d] in storage", id));
    return false;
  }
  
  private void TestPrint(final MetaTreeBranch branch, HashMap<String, Object> tree){
    if (branch == null){
      LOG.error("Branch was null!!!");
      return;
    } 
      
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("parent_hash", branch.parent_hash);
    map.put("depth", branch.depth);
    map.put("display_name", branch.display_name);
    
    if (branch.branches != null){
      HashMap<String, Object> branches = new HashMap<String, Object>();
      for (MetaTreeStoreBranch b : branch.branches){
        if (this.temp_branches.containsKey(b.hashCode())){
          TestPrint(this.temp_branches.get(b.hashCode()), branches);
        }else{
          branches.put(b.display_name, b);
        }
      }
      map.put("branches", branches);
    }
    
    if (branch.leaves != null){
      LOG.debug("Putting leaves on branch!! yayaya");
      map.put("leaves", branch.leaves);
    }
    
    
    tree.put(branch.display_name, map);
  }
  
  private void FlushTemp(final TsdbStore storage){
    for (Map.Entry<Integer, MetaTreeBranch> entry : this.temp_branches.entrySet()){
      LOG.trace("Flushing branch: " + entry.getValue());
      try {
        StoreBranch(storage, entry.getValue());
      } catch (JsonParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (JsonMappingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    this.temp_branches.clear();
  }
  
  private void CalculateMaxLevel(){
    if (this.rules == null)
      return;
    
    for (Map.Entry<Integer, TreeMap<Integer, MetaTreeRule>> crs : this.rules.entrySet()){
      for (Map.Entry<Integer, MetaTreeRule> entry : crs.getValue().entrySet()){
        MetaTreeRule rule = entry.getValue();
        if (rule.getLevel() > this.max_rule_level)
          this.max_rule_level = rule.getLevel();
      }
    }
  }
  
/* GETTERS AND SETTERS ----------------------------------- */ 
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
    this.c_name = true;
  }

  public String getNotes() {
    return notes;
  }

  public void setNotes(String notes) {
    this.notes = notes;
    this.c_notes = true;
  }

  public boolean getStrict_match() {
    return strict_match;
  }

  public void setStrict_match(boolean strict_match) {
    this.strict_match = strict_match;
    this.c_strict_match = true;
  }

  public int getTree_id() {
    return tree_id;
  }

  public void setTree_id(int tree_id) {
    this.tree_id = tree_id;
  }

  public TreeMap<Integer, TreeMap<Integer, MetaTreeRule>> getRules() {
    return rules;
  }

  public HashSet<String> getNo_matches() {
    return no_matches;
  }

  public void setNum_branches(int num_branches) {
    this.num_branches = num_branches;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(){
    this.created = System.currentTimeMillis() / 1000;
  }
  
  public long getFull_sync_start() {
    return full_sync_start;
  }

  public long getFull_sync_completed() {
    return full_sync_completed;
  }

  public long getLast_update() {
    return last_update;
  }

  @JsonIgnore
  public long getVersion() {
    return version;
  }

  @JsonIgnore
  public int getMax_depth() {
    return max_depth;
  }

  public String getTree_separator() {
    return tree_separator;
  }

  public void setTree_separator(String tree_separator) {
    this.tree_separator = tree_separator;
  }
  
/* HELPER CLASSES ----------------------------------- */  
  
  /**
   * Class used for storing branch information. 
   * This class de/serializes data to and from the backing store and is used
   * when building the tree or offering a branch for display purposes
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  public static class MetaTreeBranch implements Comparable<MetaTreeBranch>{
    int parent_hash;
    int depth;
    String name;
    String display_name;
    int tree_id;
    long tree_version;
    
    // these will be used for tree building
    @JsonIgnore
    MetaTreeLeaf leaf;
    @JsonIgnore
    MetaTreeBranch branch;
    
    // these are used for tree storage/recall
    TreeSet<MetaTreeStoreLeaf> leaves = null;// = new HashSet<Tree_Store_Leaf>();
    TreeSet<MetaTreeStoreBranch> branches = null;// = new HashSet<Tree_Store_Branch>();
    
    /**
     * Constructor that starts off with the hash of the parent
     * @param parent_hash Parent's hash ID
     */
    public MetaTreeBranch(int parent_hash){
      this.parent_hash = parent_hash;
      this.depth = 0;
      this.name = "";
      this.display_name = "";
    }
    
    /**
     * Default constructor necessary for de/serialization
     */
    public MetaTreeBranch(){
      this.parent_hash = 0;
      this.depth = 0;
      this.name = "";
      this.display_name = "";
    }
    
    /**
     * Overrides the hashcode with that of the name field
     */
    public int hashCode(){
      if (name == null || name.isEmpty())
        return 0;
      return name.hashCode();
    }
    
    /**
     * Comparator based on the "display_name" to sort branches when displaying
     */
    public int compareTo( MetaTreeBranch branch ) {
      return this.display_name.compareTo(branch.display_name);
    }
    
    public String toString(){
      return String.format("pb [%d] d [%d] name [%s] dn [%s]", this.parent_hash,
          this.depth, this.name, this.display_name);
    }
  }
  
  /**
   * Stores information about a leaf for post processing
   */
  private class MetaTreeLeaf{
    int parent_hash;
    int depth;
    String name;
    String display_name;  
    String tsuid;

    /**
     * Converts a branch to a leaf when we've reached the tip of a branch
     * @param branch Branch to convert to a leaf
     * @param m Timeseries Meta Data to fetch the TSUID from
     */
    public MetaTreeLeaf(MetaTreeBranch branch, final TimeSeriesMeta m){
      this.parent_hash = branch.parent_hash;
      this.depth = branch.depth;
      this.name = branch.name;
      this.display_name = branch.display_name;
      this.tsuid = m.getUID();
    }
    
    /**
     * Overrides the hashcode with that of the name
     */
    public int hashCode(){
      if (name != null)
        return name.hashCode();
      else
        return 0;
    }
    
    public String toString(){
      return String.format("pb [%d] dn [%s] tsuid [%s]", parent_hash, this.display_name, this.tsuid);
    }
  }

  /**
   * A simpler branch class used for storage and display purposes
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  private static class MetaTreeStoreBranch implements Comparable<MetaTreeStoreBranch> {
    int branch_id;
    String display_name;
    int num_leaves;
    int num_branches;
    
    /**
     * Default constructor necessary for de/serialization
     */
    public MetaTreeStoreBranch(){
      
    }
    
    /**
     * Creates a store branch from a full branch
     * @param b The full branch to copy data from
     */
    public MetaTreeStoreBranch(final MetaTreeBranch b){
      this.branch_id = b.hashCode();
      this.display_name = b.display_name;
      this.num_branches = b.branches == null ? 0 : b.branches.size();
      this.num_leaves = b.leaves == null ? 0 : b.leaves.size();
    }
  
    /**
     * Overrides the hashcode with the branch ID
     */
    public int hashCode() {
      return this.branch_id;
    }
    
    /**
     * Override for storing the branch in a hash table, compares on the branch ID
     * @param obj The branch to compare against
     */
    public boolean equals(Object obj){
      if (obj == null)
        return false;
      if (obj == this)
        return true;
      if (obj.getClass() != getClass())
        return false;
    
      MetaTreeStoreBranch b = (MetaTreeStoreBranch)obj;
      if (this.branch_id == b.branch_id)
        return true;
      return false;
    }
  
    /**
     * Comparator based on the "display_name" to sort branches when displaying
     */
    public int compareTo( MetaTreeStoreBranch branch ) {
      return this.display_name.compareTo(branch.display_name);
    }
  }

  /**
   * A simpler leaf class used for storage and display purposes
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  private static class MetaTreeStoreLeaf implements Comparable<MetaTreeStoreLeaf>{
    @JsonIgnore
    int hash;
    String display_name;
    String tsuid;
    
    /**
     * Default constructor necessary for de/serialization
     */
    public MetaTreeStoreLeaf(){
      
    }
    
    /**
     * Constructor that loads from a full leaf object
     * @param l Leaf to load from
     */
    public MetaTreeStoreLeaf(final MetaTreeLeaf l){
      this.hash = l.hashCode();
      this.display_name = l.display_name;
      this.tsuid = l.tsuid;
    }
  
    /**
     * Overrides the hash code with the local hash ID
     */
    public int hashCode() {
      //return this.hash;
      return this.display_name.hashCode();
    }
    
    /**
     * Overrides the equals by comparing the hash code
     */
    public boolean equals(Object obj){
      if (obj == null)
        return false;
      if (obj == this)
        return true;
      if (obj.getClass() != getClass())
        return false;
    
      return this.tsuid == ((MetaTreeStoreLeaf)obj).tsuid;
    }
  
    /**
     * Comparator based on the "display_name" to sort branches when displaying
     */
    public int compareTo( MetaTreeStoreLeaf leaf ) {
      return this.display_name.compareTo(leaf.display_name);
    }
  
    public String toString(){
      return String.format("pb [%d] dn [%s] tsuid [%s]", this.hash, this.display_name, this.tsuid);
    }
  }

  /**
   * Keeps track of the recursion for a branch/tree
   */
  private class TreeRecursionState {
    String[] splits;
    int rule_idx;
    int split_idx;
    MetaTreeBranch current_branch;
    
    /**
     * Default constructor
     */
    public TreeRecursionState(){
      splits = null;
      rule_idx = 0;
      split_idx = 0;
      current_branch = null;
    }
    
    public String toString(){
      return String.format("rule_idx [%d] split_idx [%d] have_splits [%s] branch %s", 
          this.rule_idx, this.split_idx, (this.splits == null ? "no" : "yes"), 
          this.current_branch);
    }
  }
  
  /**
   * Used to format a branch for display purposes
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  public static class MetaTreeBranchDisplay{
    int branch_id;
    int parent_branch_id;
    int num_leaves;
    int num_branches;
    String display_name;
    int tree_id;
    
    TreeSet<MetaTreeStoreLeaf> leaves = null;
    TreeSet<MetaTreeStoreBranch> branches = null;
    
    public MetaTreeBranchDisplay(){
      
    }
    
    public MetaTreeBranchDisplay(final MetaTreeBranch b){
      this.branch_id = b.hashCode();
      this.parent_branch_id = b.parent_hash;
      this.display_name = b.display_name;
      this.tree_id = b.tree_id;
      this.leaves = b.leaves;
      if (this.leaves != null)
        this.num_leaves = this.leaves.size();
      this.branches = b.branches;
      if (this.branches != null)
        this.num_branches = this.branches.size();
    }
  }
}

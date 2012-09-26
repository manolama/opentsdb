// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.JSON;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdMap;

/**
 * Handles search requests for different timeseries metadata.
 * 
 * NOTE: This does NOT return actual data points, just lets users browse
 * information about the data that's stored in the system.
 *
 */
public class SearchRPC implements HttpRpc {  
  private static final Logger LOG = LoggerFactory.getLogger(SearchRPC.class);
  
  private enum SearchOperator {
    GT,
    LT,
    GE,
    LE,
    EQ,
    NE
  }
  
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // don't bother if we don't have any tsuids to work with
    if (tsdb.ts_uids.isEmpty()){
      query.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Timeseries UID list is empty");
      return;
    }
    
    // parse the search query
    SearchQuery search_query = new SearchQuery();
    if (query.getMethod() == HttpMethod.POST){
      LOG.trace("Parsing POST data");
      JSON codec = new JSON(search_query);
      if (!codec.parseObject(query.getPostData())){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse JSON data");
        return;
      }
      search_query = (SearchQuery)codec.getObject();
      LOG.trace(codec.getJsonString());
    }else{
      LOG.trace("Parsing query string data");
      search_query = this.parseQueryString(query);
      // error already sent
      if (search_query == null)
        return;
      JSON codec = new JSON(search_query);
      LOG.trace(codec.getJsonString());
    }
    
    // validate the query first
    if (!search_query.validateQuery(query))
      return;
     
    // results will be stored by UID and object to avoid duplication
    Set<String> tag_pairs = new HashSet<String>();
    Set<String> metrics = new HashSet<String>();
    
    // compile the search query if we have one
    Pattern reg_query = null;
    if (search_query.query != null && !search_query.query.isEmpty()){
      reg_query = Pattern.compile(search_query.query);
      LOG.trace(String.format("Compiled query on [%s]", search_query.query));
    }
    
    // compile the tag regs if we have them
    Map<String, Pattern> tags = new HashMap<String, Pattern>();
    if (search_query.tags != null){
      for (Map.Entry<String, String> entry : search_query.tags.entrySet()){
        Pattern tagv_pattern = Pattern.compile(entry.getValue());
        tags.put(entry.getKey(), tagv_pattern);
      }
    }
    
    // compile the custom regs if we have them
    Map<String, Pattern> custom = new HashMap<String, Pattern>();
    if (search_query.custom != null){
      for (Map.Entry<String, String> entry : search_query.custom.entrySet()){
        Pattern custom_pattern = Pattern.compile(entry.getValue());
        custom.put(entry.getKey(), custom_pattern);
      }
    }
    
    // only search the metrics and tags we need to so we save some CPU time
    if (search_query.searchingGeneralMeta() || 
        search_query.field.compareTo("metrics") == 0)
      tsdb.metrics.search(search_query.field, reg_query, custom, metrics);
    if (search_query.searchingGeneralMeta() || 
        search_query.field.compareTo("tagk") == 0)
      tsdb.tag_names.search(search_query.field, reg_query, custom, tag_pairs);
    if (search_query.searchingGeneralMeta() || 
        search_query.field.compareTo("tagv") == 0)
      tsdb.tag_values.search(search_query.field, reg_query, custom, tag_pairs);
    
    // with the list of tagpairs, we can get a list of matching tsuids from the master
    Set<String> tsuids = UniqueIdMap.getTSUIDs(tag_pairs, tsdb.ts_uids, (short)3);
    LOG.trace(String.format("Found [%d] tsuids with [%d] tag pairs", 
        tsuids.size(), tag_pairs.size()));
    
    // match metrics against the timeseries list and add to the tsuids set
    if (metrics.size() > 0){
      for (String tsuid : tsdb.ts_uids){
        for (String metric : metrics){
          if (tsuid.substring(0, 6).compareTo(metric) == 0)
            tsuids.add(tsuid);
        }
      }
    }

    // store a quick flag for efficiency
    boolean searching_meta = search_query.searchingAnyMeta();
    
    // if we're searching meta, we need to process timeseries meta as well
    if (searching_meta)
      tsdb.searchTSMeta(search_query.field, reg_query, custom, tsuids);

    // loop through the tsuids, load meta if applicable, load short_meta, and then filter
    // and store the results in the results hash
    Map<String, Object> results = new HashMap<String, Object>();
    for (String tsuid : tsuids){
      TimeSeriesMeta ts_meta = null;
      Map<String, Object> short_meta = null;
      try{
        // load the meta if we're returning or searching
        if (search_query.return_meta || searching_meta)
          ts_meta = tsdb.getTimeSeriesMeta(UniqueId.StringtoID(tsuid));
        // always load the short meta since we'll likely return this only and it has tags
        short_meta = tsdb.getTSUIDShortMeta(tsuid);
        
      } catch (NoSuchUniqueId nsui){
        LOG.trace(nsui.getMessage());
        continue;
      }
      
      // FILTER - TAG
      if (tags.size() > 0){
        LOG.trace("Filtering tags...");
        @SuppressWarnings("unchecked")
        Map<String, String> sm_tags = (Map<String, String>)short_meta.get("tags");
        int matched = 0;
        for (Map.Entry<String, Pattern> entry : tags.entrySet()){
          for (Map.Entry<String, String> tag : sm_tags.entrySet()){
            if (tag.getKey().toLowerCase().compareTo(entry.getKey().toLowerCase()) == 0
                && entry.getValue().matcher(tag.getValue()).find()){
              LOG.trace(String.format("Matched tag [%s] on filter [%s] with value [%s]",
                  tag.getKey(), entry.getValue().toString(), tag.getValue()));
              matched++;
            }
          }
        }
        if (matched != tags.size()){
          LOG.trace(String.format("UID [%s] did not match all of the tag filters", tsuid));
          continue;
        }
      }
      
      // FILTER - Numerics
      if (search_query.numerics.size() > 0 && ts_meta != null){
        int matched = 0;
        for (Map.Entry<String, Map<SearchOperator, Double>> entry : search_query.numerics.entrySet()){
          Double meta_value;
          if (entry.getKey().compareTo("retention") == 0)
            meta_value = (double) ts_meta.getRetention();
          else if (entry.getKey().compareTo("max") == 0)
            meta_value = (double) ts_meta.getMax();
          else if (entry.getKey().compareTo("min") == 0)
            meta_value = (double) ts_meta.getMin();
          else if (entry.getKey().compareTo("interval") == 0)
            meta_value = (double) ts_meta.getInterval();
          else if (entry.getKey().compareTo("created") == 0)
            meta_value = (double) ts_meta.getFirstReceived();
          else if (entry.getKey().compareTo("last_received") == 0)
            meta_value = (double) ts_meta.getLastReceived();
          else{
            LOG.warn(String.format("Invalid field type [%s]", entry.getKey()));
            continue;
          }
          
          // get comparator and value
          SearchOperator operator = null;
          Double comparisson = null;
          for (Map.Entry<SearchOperator, Double> num : entry.getValue().entrySet()){
            operator = num.getKey();
            comparisson = num.getValue();
          }
          if (operator == null || comparisson == null){
            LOG.warn("Found a null operator or comparisson value");
            continue;
          }
          
          // compare
          switch (operator){
          case GT:
            if (meta_value > comparisson)
              matched++;
            break;
          case LT:
            if (meta_value < comparisson)
              matched++;
            break;
          case GE:
            if (meta_value >= comparisson)
              matched++;
            break;
          case LE:
            if (meta_value <= comparisson)
              matched++;
            break;
          case EQ:
            if (meta_value.equals(comparisson))
              matched++;
            break;
          case NE:
            if (!meta_value.equals(comparisson))
              matched++;
            break;
          }
        }
        if (matched != search_query.numerics.size()){
          LOG.trace(String.format("UID [%s] did not match all of the numeric filters", tsuid));
          continue;
        }
      }

      // Put the result!
      if (search_query.return_meta)
        results.put(tsuid, ts_meta);
      else
        results.put(tsuid, short_meta);
    }
    
    // build a response map and send away!
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("limit", search_query.limit);
    response.put("page", search_query.page);
    response.put("total_uids", results.size());
    response.put("total_pages", results.size() > 0 ? 
        ((results.size() / search_query.limit) + 1) : 0);
    
    // limit and page calculations    
    if (search_query.limit < 1 || results.size() <= search_query.limit){
      response.put("results", results.values());
    }else{
      LOG.trace(String.format("Limiting to [%d] uids on page [%d]", search_query.limit, search_query.page));
      SortedMap<String, Object> limited_results = new TreeMap<String, Object>();
      long start = (search_query.page > 0 ? search_query.page * search_query.limit : 0);
      long end = (search_query.page + 1) * search_query.limit;
      long uids = 0;
      for (Map.Entry<String, Object> entry : results.entrySet()){
        if (uids >= end){
          LOG.trace(String.format("Hit the limit [%d] with [%d] uids", 
              end, uids));
          break;
        }
        if (uids >= start && uids <= end){
          limited_results.put(entry.getKey(), entry.getValue());
        }
        uids++;
      }
      response.put("results", limited_results);
    }   
    JSON codec = new JSON(response);
    query.sendReply(codec.getJsonBytes());
    return;
  }
  
  /**
   * Parses the HTTP query string for the search query
   * @param query HTTP query to parse
   * @return A SearchQuery object if parsed successfully or NULL if there was an error
   * parsing a numeric field
   */
  private SearchQuery parseQueryString(final HttpQuery query){
    SearchQuery sq = new SearchQuery();
    
    // todo - tags and custom
    
    sq.created = query.getQueryStringParam("created");
    sq.retention = query.getQueryStringParam("retention");
    sq.max = query.getQueryStringParam("max");
    sq.min = query.getQueryStringParam("min");
    sq.interval = query.getQueryStringParam("interval");
    sq.last_received = query.getQueryStringParam("last_received");
    sq.field = query.getQueryStringParam("field");
    if (sq.field != null)
      sq.field = sq.field.toLowerCase();
    sq.query = query.getQueryStringParam("query");
    sq.return_meta = query.parseBoolean(query.getQueryStringParam("return_meta"));
    if (query.hasQueryStringParam("limit")){
      try{
        sq.limit = Long.parseLong(query.getQueryStringParam("limit"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return null;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        sq.page = Long.parseLong(query.getQueryStringParam("page"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the page value");
        return null;
      }
    }
    
    
    return sq;
  }
  
  /**
   * This is a helper class used for de/serializing query information and 
   * checking for valid data
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  private static class SearchQuery{
    public Map<String, String> tags;
    public Map<String, String> custom;
    public String created;
    public String retention;
    public String max;
    public String min;
    public String interval;
    public String last_received;
    public String field = "all";
    public String query;
    public long limit = 25;
    public long page = 0;
    public Boolean return_meta = false;
    
    // field -> operator -> value
    // if no operator was passed in, then we save it as eq
    public Map<String, Map<SearchOperator, Double>> numerics = 
      new HashMap<String, Map<SearchOperator, Double>>();
        
    private static Pattern operation = Pattern.compile("^([a-zA-Z]{2})\\s*?([\\d\\.-]+)");
    
    /**
     * Determines if this query needs to check GeneralMeta fields
     * Used to help cut down CPU so we don't load or process metadata unnecessarily
     * @return True if it does need to check, false if not
     */
    public final Boolean searchingGeneralMeta(){
      final String lc_field = field.toLowerCase();
      if (lc_field.compareTo("all") == 0)
        return true;
      if (lc_field.compareTo("description") == 0)
        return true;
      if (lc_field.compareTo("display_name") == 0)
        return true;
      if (lc_field.compareTo("notes") == 0)
        return true;
      if (lc_field.compareTo("custom") == 0)
        return true;
      return false;
    }
    
    /**
     * Determines if we're looking at any metadata fields, including timeseries metdata
     * @return True if it does need metadata, false if not
     */
    public final Boolean searchingAnyMeta(){
      if (custom != null && custom.size() > 0)
        return true;
      if (created != null && !created.isEmpty())
        return true;
      if (retention != null && !retention.isEmpty())
        return true;
      if (max != null && !max.isEmpty())
        return true;
      if (min != null && !min.isEmpty())
        return true;
      if (interval != null && !interval.isEmpty())
        return true;
      if (last_received != null && !last_received.isEmpty())
        return true;
      if (field != null){
        final String lc_field = field.toLowerCase();
        if (lc_field.compareTo("all") == 0)
          return true;
        if (lc_field.compareTo("description") == 0)
          return true;
        if (lc_field.compareTo("display_name") == 0)
          return true;
        if (lc_field.compareTo("notes") == 0)
          return true;
        if (lc_field.compareTo("custom") == 0)
          return true;
      }
      return false;
    }
    
    /**
     * Checks the given query to make sure the parameters are valid and builds the numeric map
     * 
     * Note that this function will return an error to the HTTP caller so if this
     * returns false, you should stop processing and return
     * 
     * @param query Http query to return errors to
     * @return True if the query is valid, false if there was an error
     */
    public final Boolean validateQuery(final HttpQuery query){
      if (field == null || field.isEmpty())
        field = "all";
      else
        field = field.toLowerCase();
      
      // check for a valid field
      if (field.compareTo("all") != 0 &&
          field.compareTo("metrics") != 0 &&
          field.compareTo("tagk") != 0 &&
          field.compareTo("tagv") != 0 &&
          field.compareTo("display_name") != 0 &&
          field.compareTo("description") != 0 &&
          field.compareTo("notes") != 0){
        LOG.warn(String.format("Invalid field [%s]", field));
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [field] value");
        return false;
      }
      
      // run through the numerics and parse them
      Map<SearchOperator, Double> search_number;
      if (created != null && !created.isEmpty()){
        search_number = this.parseNumeric(created);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [created] value");
          return false;
        }
        numerics.put("created", search_number);
      }
      
      if (retention != null && !retention.isEmpty()){
        search_number = this.parseNumeric(retention);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [retention] value");
          return false;
        }
        numerics.put("retention", search_number);
      }
      
      if (max != null && !max.isEmpty()){
        search_number = this.parseNumeric(max);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [max] value");
          return false;
        }
        numerics.put("max", search_number);
      }
      
      if (min != null && !min.isEmpty()){
        search_number = this.parseNumeric(min);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [min] value");
          return false;
        }
        numerics.put("min", search_number);
      }
      
      if (interval != null && !interval.isEmpty()){
        search_number = this.parseNumeric(interval);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [interval] value");
          return false;
        }
        numerics.put("interval", search_number);
      }
      
      if (last_received != null && !last_received.isEmpty()){
        search_number = this.parseNumeric(last_received);
        if (search_number == null){
          query.sendError(HttpResponseStatus.BAD_REQUEST, "Invalid [last_received] value");
          return false;
        }
        numerics.put("last_received", search_number);
      }
      
      return true;
    }
    
    public final SearchOperator getOperator(final String op){
      if (op.toLowerCase().compareTo("gt") == 0)
        return SearchOperator.GT;
      if (op.toLowerCase().compareTo("tt") == 0)
        return SearchOperator.LT;
      if (op.toLowerCase().compareTo("ge") == 0)
        return SearchOperator.GE;
      if (op.toLowerCase().compareTo("le") == 0)
        return SearchOperator.LE;
      if (op.toLowerCase().compareTo("eq") == 0)
        return SearchOperator.EQ;
      if (op.toLowerCase().compareTo("ne") == 0)
        return SearchOperator.NE;
      LOG.warn(String.format("Invalid operator [%s], defaulting to EQ", op));
      return SearchOperator.EQ;
    }
  
    /**
     * Attempts to parse the numeric value with an optional operator
     * 
     * If there's an operator at the front of the string, we attempt to parse it. If it's
     * an invalid operator, we default to eq.
     * 
     * Then we try to parse the number (or the whole thing if there wasn't an operator)
     * and if it parses nicely, we return a map. Otherwise we return null and the caller
     * should return an error to the user
     * 
     * @param number The number to parse with an operator at the front
     * @return An operator and value if successful, null if not
     */
    private final Map<SearchOperator, Double> parseNumeric(final String number){   
      SearchOperator operator = SearchOperator.EQ;
      String parsed_number = number; 
      
      // if we have an operator, parse it out
      Matcher groups = operation.matcher(number);
      if (groups.find() && groups.groupCount() >= 2){
        operator = this.getOperator(groups.group(1));
        parsed_number = groups.group(2);
      }
      
      // try to parse the number
      try{
        Double val = Double.parseDouble(parsed_number);
        Map<SearchOperator, Double> result = new HashMap<SearchOperator, Double>();
        result.put(operator, val);
        return result;
      } catch (NumberFormatException nfe){
        LOG.warn(String.format("Unable to parse number [%s]", parsed_number));
        return null;
      }
    }
  }
}

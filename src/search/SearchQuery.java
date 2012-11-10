// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.search;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.opentsdb.core.Annotation;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.tsd.HttpQuery;

import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class SearchQuery {
  private static final Logger LOG = LoggerFactory.getLogger(SearchQuery.class);

  public enum SearchOperator {
    GT,
    LT,
    GE,
    LE,
    EQ,
    NE
  }
  
  private Map<String, String> tags;
  private Map<String, String> custom;
  private String created;
  private String retention;
  private String max;
  private String min;
  private String interval;
  private String last_received;
  private String field = "content";
  private String query;
  private int limit = 25;
  private int page = 0;
  private boolean return_meta = false;
  private String error = "";
  private Pattern query_regex = null;
  private Map<String, Pattern> tags_compiled;
  private Map<String, Pattern> custom_compiled;
  private boolean return_tsuids = false;
  private String group = "host";
  private String sub_group = "metric";
  private boolean group_only = false;
  private boolean terms = false;
  private boolean regex = false;

  // field -> operator -> value
  // if no operator was passed in, then we save it as eq
  private Map<String, SimpleEntry<SearchOperator, Double>> numerics = 
    new HashMap<String, SimpleEntry<SearchOperator, Double>>();
      
  private static Pattern operation = Pattern.compile("^([a-zA-Z]{2})\\s*?([\\d\\.-]+)");
  
  /**
   * Overrides the hashcode to account for all of the compiled fields other than
   * the limit and page so that we don't have to worry about case or format
   * sensitivity
   */
  public int hashCode(){
    StringBuilder st = new StringBuilder();
    if (field != null)
      st.append(field);
    if (query != null)
      st.append(query);
    if (group != null)
      st.append(group);
    if (sub_group != null)
      st.append(sub_group);
    if (terms)
      st.append("t");
    
//    if (tags_compiled != null){
//      for (Map.Entry<String, Pattern> entry : tags_compiled.entrySet()){
//        st.append(entry.getKey());
//        st.append(entry.getValue().toString());
//      }
//    }
//    
//    if (custom_compiled != null){
//      for (Map.Entry<String, Pattern> entry : custom_compiled.entrySet()){
//        st.append(entry.getKey());
//        st.append(entry.getValue().toString());
//      }
//    }
//    
//    if (numerics != null){
//      for (Map.Entry<String, SimpleEntry<SearchOperator, Double>> entry : numerics.entrySet()){
//        st.append(entry.getKey());
//        st.append(entry.getValue().getKey().toString());
//        st.append(entry.getValue().getValue());
//      }
//    }
//    
    // we do NOT hash on the page or limit
    return st.toString().hashCode();
  }
  
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
   * You should always call this method BEFORE passing the search query onto any functions
   * as it will compile the regexes needed by search functions
   * 
   * If this returns false, check the error string using getError()
   * @return True if the query is valid, false if there was an error
   */
  public final Boolean validateQuery(){
    if (field == null || field.isEmpty())
      field = "all";
    else
      field = field.toLowerCase();
    
    // check for a valid field
    // todo - fix this up for lucene
//    if (field.compareTo("all") != 0 &&
//        field.compareTo("metrics") != 0 &&
//        field.compareTo("tagk") != 0 &&
//        field.compareTo("tagv") != 0 &&
//        field.compareTo("display_name") != 0 &&
//        field.compareTo("description") != 0 &&
//        field.compareTo("notes") != 0){
//      LOG.warn(String.format("Invalid field [%s]", field));
//      this.error = "Invalid [field] value";
//      return false;
//    }
    
    // compile the search query once for speed
    if (this.query == null)
      this.query = "";
    
    // compile tags and custom
    if (this.tags != null){
      this.tags_compiled = new HashMap<String, Pattern>();
      for (Map.Entry<String, String> entry : this.tags.entrySet()){
        Pattern tagv_pattern = Pattern.compile(entry.getValue());
        tags_compiled.put(entry.getKey(), tagv_pattern);
      }
    }
    
    if (this.custom != null){
      this.custom_compiled = new HashMap<String, Pattern>();
      for (Map.Entry<String, String> entry : this.custom.entrySet()){
        Pattern custom_pattern = Pattern.compile(entry.getValue());
        this.custom_compiled.put(entry.getKey(), custom_pattern);
      }
    }
    
    // run through the numerics and parse them
    if (created != null && !created.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(created);
      if (search_number == null){
        this.error = "Invalid [created] value";
        return false;
      }
      numerics.put("created", search_number);
      LOG.trace(String.format("Got created w op [%s] and num [%f]", search_number.getKey().toString(),
          search_number.getValue()));
    }
    
    if (retention != null && !retention.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(retention);
      if (search_number == null){
        this.error = "Invalid [retention] value";
        return false;
      }
      numerics.put("retention", search_number);
    }
    
    if (max != null && !max.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(max);
      if (search_number == null){
        this.error = "Invalid [max] value";
        return false;
      }
      numerics.put("max", search_number);
    }
    
    if (min != null && !min.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(min);
      if (search_number == null){
        this.error = "Invalid [min] value";
        return false;
      }
      numerics.put("min", search_number);
    }
    
    if (interval != null && !interval.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(interval);
      if (search_number == null){
        this.error = "Invalid [interval] value";
        return false;
      }
      numerics.put("interval", search_number);
    }
    
    if (last_received != null && !last_received.isEmpty()){
      SimpleEntry<SearchOperator, Double> search_number = this.parseNumeric(last_received);
      if (search_number == null){
        this.error = "Invalid [last_received] value";
        return false;
      }
      numerics.put("last_received", search_number);
    }
    
    return true;
  }
  
  /**
   * Determines the operator to use based on the string provided, defaulting to EQ
   * @param op The string to parse
   * @return The proper SearchOperator or EQ if the operator was invalid
   */
  public final SearchOperator getOperator(final String op){
    if (op.toLowerCase().compareTo("gt") == 0)
      return SearchOperator.GT;
    if (op.toLowerCase().compareTo("lt") == 0)
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

  public final boolean compare(final SearchOperator op, final Double left, final Double right){
    if (op == null){
      this.error = "Null operator";
      LOG.error(error);
      return false;
    }
    if (left == null){
      this.error = "Left-hand value was null";
      LOG.error(error);
      return false;
    }
    if (right == null){
      this.error = "Right-hand value was null";
      LOG.error(error);
      return false;
    }
    
    switch (op){
    case GT:
      if (left > right)
        return true;
      else
        return false;
    case LT:
      if (left < right)
        return true;
      else
        return false;
    case GE:
      if (left >= right)
        return true;
      else
        return false;

    case LE:
      if (left <= right)
        return true;
      else
        return false;
    case EQ:
      if (left.equals(right))
        return true;
      else
        return false;
    case NE:
      if (!left.equals(right))
        return true;
      else
        return false;
    }
    this.error = "Unrecognized comparator";
    LOG.error(error);
    return false;
  }
  
  /**
   * Parses the HTTP query string for the search query
   * @param query HTTP query to parse
   * @return A SearchQuery object if parsed successfully or NULL if there was an error
   * parsing a numeric field
   */
  public boolean parseQueryString(final HttpQuery query){
    if (query.hasQueryStringParam("tags"))
      this.tags =this.parseQueryStringList(query.getQueryStringParam("tags"));
    
    if (query.hasQueryStringParam("custom"))
      this.custom = this.parseQueryStringList(query.getQueryStringParam("custom"));
    
    if (query.hasQueryStringParam("created"))
      this.created = query.getQueryStringParam("created");
    if (query.hasQueryStringParam("retention"))
      this.retention = query.getQueryStringParam("retention");
    if (query.hasQueryStringParam("max"))
      this.max = query.getQueryStringParam("max");
    if (query.hasQueryStringParam("min"))
      this.min = query.getQueryStringParam("min");
    if (query.hasQueryStringParam("interval"))
      this.interval = query.getQueryStringParam("interval");
    if (query.hasQueryStringParam("last_received"))
      this.last_received = query.getQueryStringParam("last_received");
    if (query.hasQueryStringParam("field"))
      this.field = query.getQueryStringParam("field");
    if (query.hasQueryStringParam("query"))
      this.query = query.getQueryStringParam("query");
    if (query.hasQueryStringParam("return_meta"))
      this.return_meta = query.parseBoolean(query.getQueryStringParam("return_meta"));
    if (query.hasQueryStringParam("return_tsuids"))
      this.return_tsuids = query.parseBoolean(query.getQueryStringParam("return_tsuids"));
    if (query.hasQueryStringParam("group_only"))
      this.group_only = query.parseBoolean(query.getQueryStringParam("group_only"));
    if (query.hasQueryStringParam("terms"))
      this.terms = query.parseBoolean(query.getQueryStringParam("terms"));
    if (query.hasQueryStringParam("regex"))
      this.regex = query.parseBoolean(query.getQueryStringParam("regex"));
    if (query.hasQueryStringParam("limit")){
      try{
        this.limit = Integer.parseInt(query.getQueryStringParam("limit"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the limit value");
        return false;
      }
    }
    if (query.hasQueryStringParam("page")){
      try{
        this.page = Integer.parseInt(query.getQueryStringParam("page"));
      } catch (NumberFormatException nfe){
        query.sendError(HttpResponseStatus.BAD_REQUEST, "Unable to parse the page value");
        return false;
      }
    }
    if (query.hasQueryStringParam("group"))
      this.group = query.getQueryStringParam("group");
    if (query.hasQueryStringParam("sub_group"))
      this.sub_group = query.getQueryStringParam("sub_group");
    return true;
  }
 
  /**
   * Parses a query string parameter for key/value pairs, used for the tags and custom
   * The string should be in the format {tag1=val1,tag2=val2}
   * @param query Value of the query string to parse
   * @return Null if there was an error, a map if kvs were parsed successfully
   */
  private Map<String, String> parseQueryStringList(final String query){
    if (query == null || query.isEmpty())
      return null;
    
    final int curly = query.indexOf('{');
    String list = (curly >= 0 ? query.substring(curly+1) : query);
    list = list.replaceAll("}", "");
    
    String[] pairs = null;
    if (list.indexOf(",") < 0)
      pairs = new String[]{list};
    else
      pairs = list.split(",");
    if (pairs == null){
      LOG.warn("Unable to extract any pairs from the query string");
      return null;
    }
    
    Map<String, String> kvs = new HashMap<String, String>();
    for (String pair : pairs){
      if (pair.indexOf("=") < 0){
        LOG.warn("Missing = sign");
        continue;
      }
      
      String[] kv = pair.split("=");
      if (kv.length != 2){
        LOG.warn("Invalid tag pair, wrong number of values [" + kv.length + "]");
        continue;
      }
      kvs.put(kv[0], kv[1]);
    }
    return kvs;
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
  private final SimpleEntry<SearchOperator, Double> parseNumeric(final String number){   
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
      return new SimpleEntry<SearchOperator, Double>(operator, val);
    } catch (NumberFormatException nfe){
      LOG.warn(String.format("Unable to parse number [%s]", parsed_number));
      return null;
    }
  }
  
// GETTERS AND SETTERS ---------------------------------------
  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public Map<String, String> getCustom() {
    return custom;
  }

  public void setCustom(Map<String, String> custom) {
    this.custom = custom;
  }

  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public String getRetention() {
    return retention;
  }

  public void setRetention(String retention) {
    this.retention = retention;
  }

  public String getMax() {
    return max;
  }

  public void setMax(String max) {
    this.max = max;
  }

  public String getMin() {
    return min;
  }

  public void setMin(String min) {
    this.min = min;
  }

  public String getInterval() {
    return interval;
  }

  public void setInterval(String interval) {
    this.interval = interval;
  }

  public String getLastReceived() {
    return last_received;
  }

  public void setLastReceived(String last_received) {
    this.last_received = last_received;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    if (field != null)
      this.field = field.toLowerCase();
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
    if (query != null)
      this.query_regex = Pattern.compile(this.query);
  }
  
  public Pattern getQueryRegex(){
    return this.query_regex;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public int getPage() {
    return page;
  }

  public void setPage(int page) {
    this.page = page;
  }

  public Boolean getReturnMeta() {
    return return_meta;
  }

  public void setReturnMeta(Boolean return_meta) {
    this.return_meta = return_meta;
  }

  public String getError() {
    return error;
  }
  
  public void setError(String err){
    this.error = err;
  }

  public Map<String, SimpleEntry<SearchOperator, Double>> getNumerics() {
    return numerics;
  }

  public void setNumerics(Map<String, SimpleEntry<SearchOperator, Double>> numerics) {
    this.numerics = numerics;
  }

  public static Pattern getOperation() {
    return operation;
  }

  public static void setOperation(Pattern operation) {
    SearchQuery.operation = operation;
  }

  public Map<String, Pattern> getTagsCompiled() {
    return this.tags_compiled;
  }
  
  public Map<String, Pattern> getCustomCompiled() {
    return this.custom_compiled;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getSubGroup() {
    return sub_group;
  }

  public void setSubGroup(String sub_group) {
    this.sub_group = sub_group;
  }

  public boolean getReturnTSUIDs() {
    return return_tsuids;
  }

  public void setReturnTSUIDs(boolean return_tsuids) {
    this.return_tsuids = return_tsuids;
  }

  public boolean getGroupOnly(){
    return this.group_only;
  }
  
  public void setGroupOnly(boolean group_only){
    this.group_only = group_only;
  }

  public boolean getTerms(){
    return this.terms;
  }
  
  public void setTerms(boolean terms){
    this.terms = terms;
  }

  public boolean getRegex(){
    return this.regex;
  }
  
  public void setRegex(boolean regex){
    this.regex = regex;
  }

  public static class SearchResults {
    public int limit;
    public int page;
    public double time;
    public int total_groups;
    public int total_hits = 0;
    public int pages = 0;
    
    public SearchResults(final SearchQuery query){
      this.limit = query.getLimit();
      this.page = query.getPage();
    }
    
    // Pick one and ONLY one of these to use at any time
    public ArrayList<Map<String, Object>> short_meta;
    public ArrayList<String> tsuids;
    public ArrayList<TimeSeriesMeta> ts_meta;
    public TreeSet<String> terms;
    public Map<String, Object> groups;
    public ArrayList<Annotation> annotations;
    
    public void setTotalHits(int total_hits) {
      this.total_hits = total_hits;
      if (limit == 0)
        this.pages = 1;
      else if (this.groups != null){
        this.pages = (this.total_groups / this.limit) + 1;
      }else{
        if (total_hits > 0)
          this.pages = (total_hits / this.limit) + 1;
      }
    }
  }
}

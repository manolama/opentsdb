package net.opentsdb.tsd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.PatternSyntaxException;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.uid.NoSuchUniqueName;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataQuery {
  private static final Logger LOG = LoggerFactory.getLogger(DataQuery.class);

  public String start;
  public String end;
  public ArrayList<TSQuery> queries;
  public HashMap<String, List<String>> format_options;
  public boolean padding = false;
  public TopN topn = null;
  public boolean with_annotations = false;

  @JsonIgnore
  public String error = "";
  @JsonIgnore
  public long start_time;
  @JsonIgnore
  public long end_time;
  @JsonIgnore
  public int query_hash;
  
  @JsonIgnore
  public boolean parseQuery(final TSDB tsdb, final HttpQuery query) {
    try {
      this.start_time = query.getQueryDate(this.start);
    } catch (BadRequestException e) {
      this.error = e.getMessage();
      return false;
    }
    try {
      if (this.end != null && !this.end.isEmpty())
        this.end_time = query.getQueryDate(this.end);
    } catch (BadRequestException e) {
      this.error = e.getMessage();
      return false;
    }

    if (this.queries == null || this.queries.size() < 1) {
      this.error = "Missing queries";
      return false;
    }

    for (TSQuery q : this.queries) {
      if (!q.ParseQuery(tsdb, this.with_annotations)) {
        this.error = q.error;
        return false;
      }
    }
    return this.setQueries();
  }

  @JsonIgnore
  public boolean parseQueryString(final TSDB tsdb, final HttpQuery query) {
    this.format_options = (HashMap<String, List<String>>) query.querystring;
    
    try {
      this.start_time = query.getQueryStringDate("start");
    } catch (BadRequestException e) {
      this.error = e.getMessage();
      return false;
    }
    try {
      this.end_time = query.getQueryStringDate("end");
    } catch (BadRequestException e) {
      this.error = e.getMessage();
      return false;
    }
    if (query.hasQueryStringParam("padding"))
      this.padding = query.parseBoolean(query.getQueryStringParam("padding"));
    if (query.hasQueryStringParam("with_annotations"))
      this.with_annotations = query.parseBoolean(query.getQueryStringParam("with_annotations"));

    this.queries = new ArrayList<TSQuery>();

    final List<String> tsuids = query.getQueryStringParams("tsuids");
    if (tsuids == null || tsuids.size() < 1) {
      final List<String> ms = query.getQueryStringParams("m");
      if (ms == null) {
        this.error = "Missing parameter [m]";
        return false;
      }

      for (final String m : ms) {
        TSQuery mq = new TSQuery();
        if (!mq.parseQueryString(tsdb, m, this.with_annotations)) {
          this.error = mq.error;
          return false;
        }
        this.queries.add(mq);
      }
    } else {
      for (final String tsuid : tsuids) {
        LOG.trace("Processing TSUID: " + tsuid);
        TSQuery mq = new TSQuery();
        if (!mq.setTSUID(tsdb, tsuid)) {
          this.error = mq.error;
          return false;
        }
        this.queries.add(mq);
      }
    }

    if (query.hasQueryStringParam("topn"))
      this.parseTopN(query.getQueryStringParam("topn"));
    
    return this.setQueries();
  }
  
  public boolean setQueries(){
    // data checks
    if (this.start_time < 1) {
      throw BadRequestException.missingParameter("start");
    }
    final long now = System.currentTimeMillis() / 1000;
    if (this.end_time < 1) {
      this.end_time = now;
    }
    
    for (TSQuery q : queries) {
      try {
        q.tsd_query.setStartTime(this.start_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("start time: " + e.getMessage());
      }
      try {
        q.tsd_query.setEndTime(this.end_time);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("end time: " + e.getMessage());
      }
      q.tsd_query.setPadding(this.padding);
      
      if (this.topn != null){
        // if the query doesn't have tsuids OR groups, then we have to bail
        if ((q.tsuids == null || q.tsuids.size() < 1) && !q.has_tags){
          throw new BadRequestException("Missing a group-by tag or TSUIDs list");
        }
       
        q.tsd_query.downsample(Integer.MAX_VALUE, topn.agg);
      }
    }
    return true;
  }
  
  @JsonIgnore
  public Query[] getTSDQueries() {
    if (this.queries.size() < 1)
      return null;
    Query[] qs = new Query[this.queries.size()];
    int counter = 0;
    for (TSQuery q : this.queries) {
      qs[counter] = q.tsd_query;
      counter++;
    }
    return qs;
  }

  // PRIVATES *****************************************************
  
  private void parseTopN(final String topn){  
    TopN topper = new TopN();
    if (topn == null || topn.length() < 1){
      this.topn = topper;
      return;
    }
    
    final String[] parts = Tags.splitString(topn, ':');
    LOG.trace("Got topn: " + topn);
    if (parts.length < 1){
      LOG.warn("Missing topn components");
      return;
    }
    
    topper.agg = getAggregator(parts[0]);
    
    for (int i = 1; i<parts.length; i++){
      if (Character.isDigit(parts[i].charAt(0))){
        topper.limit = Integer.parseInt(parts[i]);
      }else{
        if (parts[i].toLowerCase().compareTo("asc") == 0)
          topper.reverse = false;
        else if (parts[i].toLowerCase().compareTo("desc") == 0)
          topper.reverse = true;
        else
          throw new BadRequestException("Invalid sorting operator");
        topper.order = parts[i].toLowerCase();
      }
    }
    
    this.topn = topper;
  }
  
  /**
   * Returns the aggregator with the given name.
   * @param name Name of the aggregator to get.
   * @throws BadRequestException if there's no aggregator with this name.
   */
  private static final Aggregator getAggregator(final String name) {
    try {
      if (name == null || name.isEmpty())
        return Aggregators.SUM;
      return Aggregators.get(name);
    } catch (NoSuchElementException e) {
      throw new BadRequestException("No such aggregation function: " + name);
    }
  }

  private static final class TSQuery {
    public String aggregator;
    public String metric;
    public ArrayList<String> tsuids;
    public HashMap<String, String> tags;
    public String type;
    public String downsample;
    public boolean agg_all;
    public boolean with_annotations = true;

    @JsonIgnore
    public Query tsd_query;
    @JsonIgnore
    public String error = "";
    @JsonIgnore
    public boolean has_tags;
    
    @JsonIgnore
    public boolean ParseQuery(final TSDB tsdb, final boolean with_annotations) {
      // set the default aggregator
      this.with_annotations = with_annotations;
      if (this.aggregator == null || this.aggregator.isEmpty())
        this.aggregator = "sum";
      final Aggregator agg = getAggregator(aggregator);

      if (this.tsuids != null && this.tsuids.size() > 0) {
        if (tsuids.size() > 1) {
          this.error = "Only one TSUID allowed per query at this time";
          return false;
        }

        this.tsd_query = tsdb.newQuery();
        try {
          this.tsd_query.setTimeSeries(this.tsuids, agg,
              (this.type != null && !this.type.isEmpty()), this.agg_all, this.with_annotations);
        } catch (NoSuchUniqueName e) {
          this.error = e.getMessage();
          return false;
        } catch (PatternSyntaxException pse) {
          this.error = pse.getMessage();
          return false;
        }
      } else {
        // parse further
        if (this.metric == null || this.metric.isEmpty()) {
          this.error = "Missing metric value";
          return false;
        }

        if (this.tags == null)
          this.tags = new HashMap<String, String>();

        this.tsd_query = tsdb.newQuery();
        try {
          this.tsd_query.setTimeSeries(this.metric, this.tags, agg,
              (this.type != null && !this.type.isEmpty()), this.agg_all, this.with_annotations);
        } catch (NoSuchUniqueName e) {
          this.error = e.getMessage();
          return false;
        } catch (PatternSyntaxException pse) {
          this.error = pse.getMessage();
          return false;
        }
      }

      if (this.downsample != null && !this.downsample.isEmpty()) {
        final int dash = this.downsample.indexOf('-', 1); // 1st char can't be
                                                          // `-'.
        if (dash < 0) {
          this.error = "Invalid downsampling specifier '" + this.downsample
              + "' in [" + this.downsample + "]";
          return false;
        }
        Aggregator downsampler;
        try {
          downsampler = Aggregators.get(this.downsample.substring(dash + 1));
        } catch (NoSuchElementException e) {
          this.error = "No such downsampling function: "
              + this.downsample.substring(dash + 1);
          return false;
        }
        final int interval = HttpQuery.parseDuration(this.downsample.substring(
            0, dash));
        this.tsd_query.downsample(interval, downsampler);
      }

      return true;
    }

    @JsonIgnore
    public boolean setTSUID(final TSDB tsdb, final String tsuid) {
      this.tsuids = new ArrayList<String>();
      final String[] parts = Tags.splitString(tsuid, ':');
      int i = parts.length;
      if (i < 2 || i > 4) {
        this.error = "Invalid parameter tsuids=" + tsuid + " ("
            + (i < 2 ? "not enough" : "too many") + " :-separated parts)";
        return false;
      }
      final Aggregator agg = getAggregator(parts[0]);
      i--; // Move to the last part (the metric name).
      final String[] split_tsuids = Tags.splitString(parts[i], ',');
      if (split_tsuids == null || split_tsuids.length < 1) {
        this.error = "Unable to extract Timeseries UID";
        return false;
      }
      final ArrayList<String> tsuids = new ArrayList<String>();
      for (String ts : split_tsuids)
        tsuids.add(ts);
      final boolean rate = "rate".equals(parts[--i]);

      this.tsd_query = tsdb.newQuery();
      try {
        this.tsd_query.setTimeSeries(tsuids, agg, rate, this.agg_all, this.with_annotations);
      } catch (NoSuchUniqueName e) {
        this.error = e.getMessage();
        return false;
      } catch (PatternSyntaxException pse) {
        this.error = pse.getMessage();
        return false;
      }
      if (rate) {
        i--; // Move to the next part.
      }

      // downsampling function & interval.
      if (i > 0) {
        final int dash = parts[1].indexOf('-', 1); // 1st char can't be `-'.
        if (dash < 0) {
          this.error = "Invalid downsampling specifier '" + parts[1]
              + "' in tsuids=" + tsuid;
          return false;
        }
        Aggregator downsampler;
        try {
          downsampler = Aggregators.get(parts[1].substring(dash + 1));
        } catch (NoSuchElementException e) {
          this.error = "No such downsampling function: "
              + parts[1].substring(dash + 1);
          return false;
        }
        final int interval = HttpQuery.parseDuration(parts[1]
            .substring(0, dash));
        this.tsd_query.downsample(interval, downsampler);
      }

      this.tsuids.add(tsuid);
      return true;
    }

    @JsonIgnore
    public boolean parseQueryString(final TSDB tsdb, final String query, final boolean with_annotations) {
      this.with_annotations = with_annotations;
      // m is of the following forms:
      // agg:[interval-agg:][rate:]metric[{tag=value,...}]
      // Where the parts in square brackets `[' .. `]' are optional.
      final String[] parts = Tags.splitString(query, ':');
      int i = parts.length;
      if (i < 2 || i > 5) {
        this.error = "Invalid parameter m=" + query + " ("
            + (i < 2 ? "not enough" : "too many") + " :-separated parts)";
        return false;
      }

      this.tsd_query = tsdb.newQuery();
      final Aggregator agg = getAggregator(parts[0]);
      i--; // Move to the last part (the metric name).
      this.tags = new HashMap<String, String>();
      final String metric = Tags.parseWithMetric(parts[i], this.tags);

      boolean rate = false;
      boolean agg_all = false;

      for (int x = 1; x < parts.length - 1; x++) {
        if (parts[x].toLowerCase().equals("rate"))
          rate = true;
        else if (parts[x].toLowerCase().equals("agg"))
          agg_all = true;
        else if (Character.isDigit(parts[x].charAt(0))) {
          Aggregator downsampler;
          final int dash = parts[1].indexOf('-', 1);
          try {
            if (dash < 0) {
              this.error = "Invalid downsampling specifier '" + parts[1]
                  + "' in m=" + query;
              return false;
            }
            downsampler = Aggregators.get(parts[1].substring(dash + 1));
          } catch (NoSuchElementException e) {
            this.error = "No such downsampling function: "
                + parts[1].substring(dash + 1);
            return false;
          }
          final int interval = HttpQuery.parseDuration(parts[1].substring(0,
              dash));
          this.tsd_query.downsample(interval, downsampler);
        }
      }

      try {
        if (this.tags.size() > 0){
          this.has_tags = true;
          LOG.trace("Definitely has the damned tags");
        }
        this.tsd_query.setTimeSeries(metric, tags, agg, rate, agg_all, this.with_annotations);
      } catch (NoSuchUniqueName e) {
        this.error = e.getMessage();
        return false;
      } catch (PatternSyntaxException pse) {
        this.error = pse.getMessage();
        return false;
      }
      return true;
    }
  }
  
  public static final class TopN {
    public String aggregator = "sum";
    public String order = "desc";
    public int limit = 10;
    
    @JsonIgnore
    public Aggregator agg = Aggregators.SUM;
    @JsonIgnore
    public boolean reverse = true;
  }
}

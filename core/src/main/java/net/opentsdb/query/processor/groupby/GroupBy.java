package net.opentsdb.query.processor.groupby;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.processor.TimeSeriesProcessor;
import net.opentsdb.query.processor.TimeSeriesProcessorConfig;
import net.opentsdb.utils.Deferreds;

public class GroupBy extends TimeSeriesProcessor {

  private Map<TimeSeriesGroupId, List<TimeSeriesIterator<?>>> grouped;
  
  public GroupBy(final QueryContext context,
                 final TimeSeriesProcessorConfig<GroupBy> config) {
    super(context, config);
  }
  
  @Override
  public Deferred<Object> initialize() {
    grouped = Maps.newHashMap();
    final List<Deferred<Object>> deferreds = Lists.newArrayList();
    final TimeSeriesQuery query = ((GroupByConfig) config).query();
    // mmm on^2 albeit for small ns
    for (final IteratorGroup group : iterators.groups()) {
      for (final Metric metric : query.getMetrics()) {
        if (group.id().id().equals(metric.getId())) {
          if (!Strings.isNullOrEmpty(metric.getFilter())) {
            group(query, metric, group, deferreds);
          }
          break;
        }
      }
    }
    
    class InitCB implements Callback<Deferred<Object>, Object> {
      @Override
      public Deferred<Object> call(final Object result_or_exception) throws Exception {
        if (result_or_exception instanceof Exception) {
          init_deferred.callback((Exception) result_or_exception);
          return init_deferred;
        }
        
        iterators = new DefaultIteratorGroups();
        for (final Entry<TimeSeriesGroupId, List<TimeSeriesIterator<?>>> entry : 
            grouped.entrySet()) {
          for (final TimeSeriesIterator<?> it : entry.getValue()) {
            iterators.addIterator(entry.getKey(), it);
          }
        }
        
        init_deferred.callback(null);
        return init_deferred;
      }
    }
    
    Deferred.group(deferreds)
      .addCallback(Deferreds.NULL_GROUP_CB)
      .addBoth(new InitCB());
    return init_deferred;
  }
  
  @Override
  public TimeSeriesProcessor getClone(QueryContext context) {
    // TODO Auto-generated method stub
    return null;
  }

  void group(final TimeSeriesQuery query, final Metric metric, final IteratorGroup group, final List<Deferred<Object>> deferreds) {
    // TODO - need metrics and tags sorted!!!!!
    final Aggregator aggregator;
    if (!Strings.isNullOrEmpty(metric.getAggregator())) {
      aggregator = Aggregators.get(metric.getAggregator());
    } else {
      aggregator = Aggregators.get(query.getTime().getAggregator());
    }
    System.out.println("GOT AGG: " + aggregator);
    Filter filter_set = null;
    for (final Filter filters : query.getFilters()) {
      if (filters.getId().equals(metric.getFilter())) {
        filter_set = filters;
        break;
      }
    }
    System.out.println("GOT FILTER SET: " + filter_set.getId());
    
    final Set<String> keys = Sets.newHashSet();
    for (TagVFilter filter : filter_set.getTags()) {
      if (filter.isGroupBy()) {
        keys.add(filter.getTagk());
      }
    }
    
    System.out.println("GROUPING ON KEYS: " + keys);
    
    final Map<String, GroupByNumericIterator> groups = Maps.newHashMap();
    for (final TimeSeriesIterator<?> iterator : group.flattenedIterators()) {
      if (iterator.type() != NumericType.TYPE) {
        // TODO - handle types
        continue;
      }
      
      // TODO - handle actual bytes and optimize. This be ugly!
      final StringBuilder buf = new StringBuilder();
      for (final byte[] namespace : iterator.id().namespaces()) {
        buf.append(new String(namespace, Const.UTF8_CHARSET));
      }
      for (final byte[] m : iterator.id().metrics()) {
        buf.append(new String(m, Const.UTF8_CHARSET));
      }
      boolean matched = true;
      for (final String key : keys) {
        final byte[] tagv = iterator.id().tags().get(key.getBytes(Const.UTF8_CHARSET));
        if (tagv == null) {
          System.out.println("DROPPING!: " + iterator.id());
          matched = false;
          break;
        }
        buf.append(new String(tagv, Const.UTF8_CHARSET));
      }
      if (!matched) {
        continue;
      }
      
      final String key = buf.toString();
      System.out.println(key);
      GroupByNumericIterator g = groups.get(key);
      if (g == null) {
        g = new GroupByNumericIterator(context, (GroupByConfig) config, aggregator);
        g.addIterator((TimeSeriesIterator<NumericType>) iterator);
        groups.put(key, g);
      } else {
        g.addIterator((TimeSeriesIterator<NumericType>) iterator);
      }
    }
    
    System.out.println("Groups: " + groups.size());
    for (GroupByNumericIterator g : groups.values()) {
      List<TimeSeriesIterator<?>> its = grouped.get(group.id());
      if (its == null) {
        its = Lists.newArrayList();
        grouped.put(group.id(), its);
      }
      its.add(g);
      deferreds.add(g.initialize());
    }
  }
  
}

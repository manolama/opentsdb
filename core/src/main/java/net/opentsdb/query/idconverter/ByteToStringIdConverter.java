// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.idconverter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.TypedTimeSeriesIterator;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseWrappedQueryResult;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xPartialTimeSeriesSet;

/**
 * Simply converts byte encoded IDs to their strings using the 
 * data store associated with each. For string ID results, they're just
 * passed upstream.
 * 
 * @since 3.0
 */
public class ByteToStringIdConverter extends AbstractQueryNode {

  /** The config. */
  private final ByteToStringIdConverterConfig config;
  
  protected Map<String, TLongObjectMap<TimeSeriesId>> decoded_ids;
  protected Map<String, TLongObjectMap<PartialTimeSeriesSet>> sets;
  
  /**
   * Default ctor.
   * @param factory The parent factory.
   * @param context The non-null query context.
   * @param config The non-null config.
   */
  public ByteToStringIdConverter(final QueryNodeFactory factory,
                                 final QueryPipelineContext context,
                                 final ByteToStringIdConverterConfig config) {
    super(factory, context);
    this.config = config;
    decoded_ids = Maps.newConcurrentMap();//new TLongObjectHashMap<TimeSeriesId>();
    sets = Maps.newConcurrentMap();
  }

  @Override
  public QueryNodeConfig config() {
    return config;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public void onNext(final QueryResult next) {
    if (next.idType() == Const.TS_STRING_ID ||
        next.timeSeries().isEmpty()) {
      sendUpstream(next);
      return;
    }
    
    // conversion time!
    final List<Deferred<TimeSeriesStringId>> deferreds = 
        Lists.newArrayListWithExpectedSize(next.timeSeries().size());
    for (final TimeSeries series : next.timeSeries()) {
      deferreds.add(((TimeSeriesByteId) 
          series.id()).dataStore().resolveByteId(
              (TimeSeriesByteId) series.id(), null /* TODO */));
    }
    
    class ResolveCB implements Callback<Void, ArrayList<TimeSeriesStringId>> {

      @Override
      public Void call(final ArrayList<TimeSeriesStringId> ids) throws Exception {
        sendUpstream(new ConvertedResult(next, ids));
        return null;
      }
      
    }
    
    class ErrorCB implements Callback<Void, Exception> {

      @Override
      public Void call(final Exception e) throws Exception {
        sendUpstream(e);
        return null;
      }
      
    }
    
    Deferred.groupInOrder(deferreds)
      .addCallbacks(new ResolveCB(), new ErrorCB());
  }

  @Override
  public void onNext(final PartialTimeSeries next) {
    if (next.set().timeSeriesCount() < 1 ||
        !((ByteToStringIdConverterConfig) config).data_sources.containsKey(next.set().dataSource())) {      
      // TODO - make sure expressions work since the source ID changes. But it
      // return strings.
      sendUpstream(next);
      return;
    }
    
    TLongObjectMap<TimeSeriesId> ids = null;
    PartialTimeSeriesSet set = null;
    synchronized (sets) {
      TLongObjectMap<PartialTimeSeriesSet> set_map = sets.get(next.set().dataSource());
      if (set_map == null) {
        ids = new TLongObjectHashMap<TimeSeriesId>();
        decoded_ids.put(next.set().dataSource(), ids);
        
        set = new WrappedPartialTimeSeriesSet(next.set(), ids);
        set_map = new TLongObjectHashMap<PartialTimeSeriesSet>();
        set_map.put(set.start().epoch(), set);
        sets.put(next.set().dataSource(), set_map);
      } else {
        ids = decoded_ids.get(next.set().dataSource()); // must be there
        set = set_map.get(next.set().start().epoch());
        if (set == null) {
          set = new WrappedPartialTimeSeriesSet(next.set(), ids);
          set_map.put(next.set().start().epoch(), set);
        }
      }
    }
    
    System.out.println("        SERIES: " + next.set().timeSeriesCount());
    // now decode then pop upstream when ready
    TimeSeriesId id = next.set().id(next.idHash());
    if (id == null) {
      sendUpstream(new RuntimeException("Ooops! no ID for: " + next.idHash()));
    }
    
    class CB implements Callback<Void, TimeSeriesStringId> {
      final TLongObjectMap<TimeSeriesId> ids;
      final PartialTimeSeriesSet set;
      
      CB(final TLongObjectMap<TimeSeriesId> ids, final PartialTimeSeriesSet set) {
        this.ids = ids;
        this.set = set;
      }
      
      @Override
      public Void call(final TimeSeriesStringId id) throws Exception {
        synchronized (ids) {
          ids.put(next.idHash(), id);
        }
        sendUpstream(new WrappedPartialTimeSeries(next, set));
        return null;
      }
    }
    
    ((TimeSeriesByteId) id).dataStore().resolveByteId(
            (TimeSeriesByteId) id, null /* TODO */)
      .addCallback(new CB(ids, set));
  }
  
  /** Simple wrapped result. */
  class ConvertedResult extends BaseWrappedQueryResult {

    private final List<TimeSeries> wrapped_series;
    
    public ConvertedResult(final QueryResult result, 
                           final List<TimeSeriesStringId> ids) {
      super(result);
      wrapped_series = Lists.newArrayListWithExpectedSize(result.timeSeries().size());
      int index = 0;
      // Invariate: the number of ids must match the time series AND the
      // order of iteration must be the same every time it's called.
      for (final TimeSeries series : result.timeSeries()) {
        wrapped_series.add(new ConvertedTimeSeries(ids.get(index++), series));
      }
    }
    
    @Override
    public Collection<TimeSeries> timeSeries() {
      return wrapped_series;
    }
    
    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public QueryNode source() {
      return ByteToStringIdConverter.this;
    }
    
  }
  
  /** Overloads the ID. */
  class ConvertedTimeSeries implements TimeSeries {

    private final TimeSeriesId id;
    private final TimeSeries source;
    
    ConvertedTimeSeries(final TimeSeriesId id, final TimeSeries source) {
      this.id = id;
      this.source = source;
    }
    
    @Override
    public TimeSeriesId id() {
      return id;
    }

    @Override
    public Optional<TypedTimeSeriesIterator> iterator(
        final TypeToken<? extends TimeSeriesDataType> type) {
      return source.iterator(type);
    }

    @Override
    public Collection<TypedTimeSeriesIterator> iterators() {
      return source.iterators();
    }

    @Override
    public Collection<TypeToken<? extends TimeSeriesDataType>> types() {
      return source.types();
    }

    @Override
    public void close() {
      source.close();
    }
    
  }

  class WrappedPartialTimeSeries implements PartialTimeSeries {
    final PartialTimeSeries source;
    final PartialTimeSeriesSet set;
    
    WrappedPartialTimeSeries(final PartialTimeSeries source, final PartialTimeSeriesSet set) {
      this.source = source;
      this.set = set;
    }
    
    @Override
    public void close() throws Exception {
      source.close();
    }

    @Override
    public long idHash() {
      return source.idHash();
    }

    @Override
    public PartialTimeSeriesSet set() {
      return set;
    }

    @Override
    public TypeToken<? extends TimeSeriesDataType> getType() {
      return source.getType();
    }

    @Override
    public Object data() {
      return source.data();
    }
    
  }
  
  class WrappedPartialTimeSeriesSet implements PartialTimeSeriesSet {

    final PartialTimeSeriesSet source;
    TLongObjectMap<TimeSeriesId> decoded_ids;
    
    WrappedPartialTimeSeriesSet(final PartialTimeSeriesSet source,
        TLongObjectMap<TimeSeriesId> decoded_ids) {
      this.source = source;
      this.decoded_ids = decoded_ids;
    }
    
    @Override
    public void close() throws Exception {
      source.close();
    }

    @Override
    public int totalSets() {
      return source.totalSets();
    }

    @Override
    public boolean complete() {
      return source.complete();
    }

    @Override
    public QueryNode node() {
      return source.node();
    }

    @Override
    public String dataSource() {
      return source.dataSource();
    }

    @Override
    public TimeStamp start() {
      return source.start();
    }

    @Override
    public TimeStamp end() {
      return source.end();
    }

    @Override
    public TimeSeriesId id(long hash) {
      return decoded_ids.get(hash);
    }

    @Override
    public int timeSeriesCount() {
      return source.timeSeriesCount();
    }

    @Override
    public TimeSpecification timeSpecification() {
      return source.timeSpecification();
    }
    
  }
}

package net.opentsdb.query.joins;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;
import net.opentsdb.utils.Pair;

public class JoinedTimeSeries implements TimeSeries {
  final JoinType type;
  TimeSeriesId joined_id;
  final Pair<TimeSeries, TimeSeries> pair;
  final String alias;
  final JoinIteratorFactory factory;
  
  public JoinedTimeSeries(final JoinType type, 
      final Pair<TimeSeries, TimeSeries> pair, 
      final String alias,
      final JoinIteratorFactory factory) {
    this.type = type;
    this.pair = pair;
    this.alias = alias;
    this.factory = factory;
  }

  @Override
  public TimeSeriesId id() {
    if (joined_id == null) {
      if (pair.getKey() != null) {
        if (pair.getKey().id().type() == Const.TS_BYTE_ID) {
          joined_id = new JoinedByteId();
        } else {
          joined_id = new JoinedStringId();
        }
      } else {
        if (pair.getValue().id().type() == Const.TS_BYTE_ID) {
          joined_id = new JoinedByteId();
        } else {
          joined_id = new JoinedStringId();
        }
      }
    }
    return joined_id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      TypeToken<?> type) {
    Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> left = 
        pair.getKey() == null ? Optional.empty() : pair.getKey().iterator(type);
    Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> right = 
        pair.getValue() == null ? Optional.empty() : pair.getValue().iterator(type);
    if (left.isPresent() || right.isPresent()) {
      return Optional.of(factory.newIterator(left.isPresent() ? left.get() : null, 
          right.isPresent() ? right.get() : null, 
              this));
    }
    return Optional.empty();
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
  
  // at this point we're assured the two types are the same.
  class JoinedByteId implements TimeSeriesByteId {
    ByteMap<byte[]> tags;
    
    @Override
    public boolean encoded() {
      return false;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      return Const.TS_BYTE_ID;
    }

    @Override
    public long buildHashCode() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int compareTo(TimeSeriesByteId o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public TimeSeriesDataStore dataStore() {
      return pair.getKey() == null ? 
          ((TimeSeriesByteId) pair.getValue().id()).dataStore() :
          ((TimeSeriesByteId) pair.getKey().id()).dataStore();
    }

    @Override
    public byte[] alias() {
      return alias.getBytes(Const.UTF8_CHARSET);
    }

    @Override
    public byte[] namespace() {
      if (pair.getKey() == null || type == JoinType.RIGHT) {
        return ((TimeSeriesByteId) pair.getValue().id()).namespace();
      } else {
        return ((TimeSeriesByteId) pair.getKey().id()).namespace();
      }
    }

    @Override
    public byte[] metric() {
      if (pair.getKey() == null || type == JoinType.RIGHT) {
        return ((TimeSeriesByteId) pair.getValue().id()).metric();
      } else {
        return ((TimeSeriesByteId) pair.getKey().id()).metric();
      }
    }

    @Override
    public ByteMap<byte[]> tags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<byte[]> aggregatedTags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<byte[]> disjointTags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ByteSet uniqueIds() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Deferred<TimeSeriesStringId> decode(boolean cache, Span span) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }

  class JoinedStringId implements TimeSeriesStringId {

    @Override
    public boolean encoded() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> type() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public long buildHashCode() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int compareTo(TimeSeriesStringId o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String alias() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String namespace() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String metric() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Map<String, String> tags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> aggregatedTags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List<String> disjointTags() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Set<String> uniqueIds() {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
}

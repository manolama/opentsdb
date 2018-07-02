package net.opentsdb.query.processor.expressions;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.TimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes.ByteMap;

public class ExpressionTimeSeries implements TimeSeries {

  final BinaryExpressionNode node;
  final TimeSeries left;
  final TimeSeries right;
  final TimeSeriesId id;
  
  public ExpressionTimeSeries(BinaryExpressionNode node,
      TimeSeries left, TimeSeries right) {
    this.node = node;
    this.left = left;
    this.right = right;
    if (left != null && right != null) {
      // TODO  return joined ID;
      //id = null;
      if (left.id().type() == Const.TS_BYTE_ID) {
        id = new ByteIdOverride((TimeSeriesByteId) left.id());
      } else {
        id = new StringIdOverride((TimeSeriesStringId) left.id());
      }
    } else if (left == null) {
      if (right.id().type() == Const.TS_BYTE_ID) {
        id = new ByteIdOverride((TimeSeriesByteId) right.id());
      } else {
        id = new StringIdOverride((TimeSeriesStringId) right.id());
      }
    } else {
      if (left.id().type() == Const.TS_BYTE_ID) {
        id = new ByteIdOverride((TimeSeriesByteId) left.id());
      } else {
        id = new StringIdOverride((TimeSeriesStringId) left.id());
      }
    }
  }
  
  @Override
  public TimeSeriesId id() {
    return id;
  }

  @Override
  public Optional<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterator(
      TypeToken<?> type) {
    // TODO Auto-generated method stub
    if (type == NumericType.TYPE) {
      System.out.println(" RET NUMERIC");
      return Optional.of(new ExpressionNumericTypeIterator(this));
    }
    return Optional.empty();
  }

  @Override
  public Collection<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators() {
    // TODO Auto-generated method stub
    List<Iterator<TimeSeriesValue<? extends TimeSeriesDataType>>> iterators = Lists.newArrayListWithExpectedSize(1);
    iterators.add(new ExpressionNumericTypeIterator(this));
    return iterators;
  }

  @Override
  public Collection<TypeToken<?>> types() {
    // TODO - more types
    return Lists.newArrayList(NumericType.TYPE);
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  class ByteIdOverride implements TimeSeriesByteId {
    final TimeSeriesByteId id;
    ByteIdOverride(final TimeSeriesByteId id) {
      this.id = id;
    }
    
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
    public int compareTo(TimeSeriesByteId o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public TimeSeriesDataStore dataStore() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public byte[] alias() {
      return node.id().getBytes(Const.UTF8_CHARSET);
    }

    @Override
    public byte[] namespace() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public byte[] metric() {
      // TODO Auto-generated method stub
      return null;
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
  
  class StringIdOverride implements TimeSeriesStringId {
    final TimeSeriesStringId id;
    StringIdOverride(final TimeSeriesStringId id) {
      this.id = id;
    }
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
      return node.id();
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

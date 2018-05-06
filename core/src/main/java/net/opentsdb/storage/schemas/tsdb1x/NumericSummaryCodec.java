package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.types.numeric.NumericSummaryType;

public class NumericSummaryCodec implements Codec {

  @Override
  public TypeToken<? extends TimeSeriesDataType> type() {
    return NumericSummaryType.TYPE;
  }

  @Override
  public Span<? extends TimeSeriesDataType> newSequences(boolean reversed) {
    return new NumericSummarySpan(reversed);
  }

  @Override
  public RowSeq newRowSeq(long base_time) {
    // TODO Auto-generated method stub
    return null;
  }

}

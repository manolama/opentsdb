package net.opentsdb.query.joins;

import java.util.Arrays;
import java.util.List;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.query.joins.Joiner.Operand;

public class SingleResultJoin extends BaseJoin {

  // move everything to the left iterator to keep things simple.
  final Operand operand;
  
  protected SingleResultJoin(final BaseHashedJoinSet join) {
    super(join);
    if (join.left_map != null) {
      operand = Operand.LEFT;
    } else if (join.right_map != null) {
      operand = Operand.RIGHT;
      left_iterator = right_iterator;
    } else {
      operand = Operand.CONDITION;
      left_iterator = ternary_iterator;
    }
    System.out.println(" LEFT: " + join.left_map.size() + "  R: " + join.right_map);
    advance();
  }

  @Override
  protected void advance() {
    while (true) {
      if (left_series != null && left_idx < left_series.size()) {
        TimeSeries value = left_series.get(left_idx);
        if (value == null) {
          left_idx++;
          continue;
        }
        switch(operand) {
        case LEFT:
          next[0] = value;
          break;
        case RIGHT:
          next[1] = value;
          break;
        case CONDITION:
          next[2] = value;
          break;
        }
        left_idx++;
        return;
      }
    
      if (!left_iterator.hasNext()) {
        next = null;
        return;
      }
      
      left_iterator.advance();
      left_series = left_iterator.value();
      left_idx = 0;
      if (left_series == null) {
        return;
      }
    }
  }

  @Override
  protected void ternaryAdvance() {
    // no-op, we use Advance here for everything.
    advance();
  }

}

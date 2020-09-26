package net.opentsdb.query.processor.bucketpercentile;

import java.io.Closeable;

import net.opentsdb.data.TimeSeries;

public abstract class Computer implements Closeable {

  final int index;
  
  Computer(final int index) {
    this.index = index;
  }
  
  abstract void run();
  
  abstract TimeSeries getSeries(final int percentile_index);
}

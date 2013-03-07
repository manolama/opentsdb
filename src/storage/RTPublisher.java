package net.opentsdb.storage;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Annotation;
import net.opentsdb.meta.GeneralMeta;
import net.opentsdb.meta.TimeSeriesMeta;

public abstract class RTPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(RTPublisher.class);
  protected enum DataType{
    DataPoint,
    Annotation,
    TimeseriesMeta
  }
  
  protected final ArrayBlockingQueue<DataPoint> datapoint_queue;
  protected final ArrayBlockingQueue<Annotation> annotation_queue;
  protected final ArrayBlockingQueue<TimeSeriesMeta> ts_meta_queue;
  protected final ArrayBlockingQueue<GeneralMeta> general_meta_queue;
  
  protected final AtomicInteger datapoint_queue_full = new AtomicInteger();
  protected final AtomicInteger annotation_queue_full = new AtomicInteger();
  protected final AtomicInteger ts_meta_queue_full = new AtomicInteger();
  protected final AtomicInteger general_meta_queue_full = new AtomicInteger();
  
  protected boolean datapoint_put_fail = false;
  protected boolean annotation_put_fail = false;
  protected boolean ts_meta_put_fail = false;
  protected boolean general_meta_put_fail = false;
  
  protected RTDatapointWorker[] datapoint_workers;
  protected RTAnnotationWorker[] annotation_workers;
  protected RTDatapointWorker[] ts_meta_workers;
  protected RTDatapointWorker[] general_meta_workers;
  
  public RTPublisher(){
    datapoint_queue = new ArrayBlockingQueue<DataPoint>(10000);
    annotation_queue = new ArrayBlockingQueue<Annotation>(100);
    ts_meta_queue = new ArrayBlockingQueue<TimeSeriesMeta>(1000);
    general_meta_queue = new ArrayBlockingQueue<GeneralMeta>(100);
  }
  
  public boolean publishDatapoint(final String metric,
      final long timestamp,
      final Object value,
      final Map<String, String> tags, 
      final String tsuid){
    
    DataPoint dp = new DataPoint(metric, timestamp, value, tags, tsuid);
    if (!this.datapoint_queue.offer(dp)){
      if (!this.datapoint_put_fail){
        LOG.error("DataPoint queue is full");
        this.datapoint_put_fail = true;
      }
      this.datapoint_queue_full.incrementAndGet();
      return false;
    }else
      this.datapoint_put_fail = false;
    return true;
  }
  
  protected abstract void initializeWorkers();
  
  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  protected static class DataPoint{
    final String metric;
    final long timestamp;
    final Object value;
    final Map<String, String> tags;
    final String tsuid;
    
    public DataPoint(){
      metric = "";
      timestamp = 0;
      value = null;
      tags = null;
      tsuid = null;
    }
    
    public DataPoint(final String metric,
        final long timestamp,
        final Object value,
        final Map<String, String> tags, 
        final String tsuid){
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
      this.tsuid = tsuid;
    } 
  }
  
  protected abstract class RTDatapointWorker extends Thread {
    protected final ArrayBlockingQueue<DataPoint> datapoint_queue;
    
    public RTDatapointWorker(final ArrayBlockingQueue<DataPoint> datapoint_queue){
      this.datapoint_queue = datapoint_queue;
    }
  }
  
  protected abstract class RTAnnotationWorker extends Thread {
    
  }
}

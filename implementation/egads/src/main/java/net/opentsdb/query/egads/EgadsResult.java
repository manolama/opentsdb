package net.opentsdb.query.egads;

import java.time.Duration;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.data.SecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.egads.olympicscoring.OlympicScoringNode;
import net.opentsdb.rollup.RollupConfig;

public class EgadsResult implements QueryResult, TimeSpecification {
  private final QueryNode node;
  private final String data_source;
  private final TimeStamp start;
  private final TimeStamp end;
  private final List<TimeSeries> series;
  private final TypeToken<? extends TimeSeriesId> id_type;

  public EgadsResult(final QueryNode node, 
                     final String data_source,
                     final TimeStamp start, 
                     final TimeStamp end, 
                     final List<TimeSeries> series,
                     final TypeToken<? extends TimeSeriesId> id_type) {
    this.node = node;
    this.data_source = data_source;
    this.start = start;
    this.end = end;
    this.series = series;
    this.id_type = id_type;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return this;
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return series;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public String dataSource() {
    return data_source;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return id_type;
  }

  @Override
  public ChronoUnit resolution() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RollupConfig rollupConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean processInParallel() {
    return false;
  }

  @Override
  public TimeStamp start() {
    return start;
  }

  @Override
  public TimeStamp end() {
    return end;
  }

  @Override
  public TemporalAmount interval() {
    // TODO Auto-generated method stub
    return Duration.ofSeconds(60);
  }

  @Override
  public String stringInterval() {
    // TODO Auto-generated method stub
    return "1m";
  }

  @Override
  public ChronoUnit units() {
    // TODO Auto-generated method stub
    return ChronoUnit.MINUTES;
  }

  @Override
  public ZoneId timezone() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateTimestamp(int offset, TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void nextTimestamp(TimeStamp timestamp) {
    // TODO Auto-generated method stub
    
  }
  
}

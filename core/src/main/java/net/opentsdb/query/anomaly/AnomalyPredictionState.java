package net.opentsdb.query.anomaly;

public class AnomalyPredictionState {

  public static enum State {
    RUNNING,
    COMPLETE,
    ERROR
  }
  
  public String host;
  public long startTime;
  public long predictionStartTime;
  public State state;
  public long hash;
}

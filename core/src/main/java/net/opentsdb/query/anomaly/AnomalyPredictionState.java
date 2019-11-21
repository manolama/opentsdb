package net.opentsdb.query.anomaly;

import java.util.Objects;

import net.opentsdb.utils.JSON;

public class AnomalyPredictionState {

  public static enum State {
    RUNNING,
    COMPLETE,
    ERROR
  }
  
  public String host;
  public long startTime;
  public long predictionStartTime;
  public long lastUpdateTime;
  public State state;
  public long hash;
  public String exception;
  
  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (o == this) {
      return true;
    }
    if (!(o instanceof AnomalyPredictionState)) {
      return false;
    }
    
    AnomalyPredictionState other = (AnomalyPredictionState) o;
    return Objects.equals(host, other.host) &&
        Objects.equals(startTime, other.startTime) &&
        Objects.equals(predictionStartTime, other.predictionStartTime) &&
        Objects.equals(lastUpdateTime, other.lastUpdateTime) &&
        Objects.equals(state, other.state) &&
        Objects.equals(hash, other.hash);
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
}

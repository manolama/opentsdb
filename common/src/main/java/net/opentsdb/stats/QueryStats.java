package net.opentsdb.stats;

public interface QueryStats {

  public Tracer tracer();

  public Span querySpan();
}

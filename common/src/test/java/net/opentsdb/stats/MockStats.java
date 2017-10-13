package net.opentsdb.stats;

public class MockStats implements QueryStats {
  private Tracer tracer;
  private Span query_span;
  
  public MockStats(Tracer tracer, Span span) {
    this.tracer = tracer;
    query_span = span;
  }
  
  @Override
  public Tracer tracer() {
    return tracer;
  }

  @Override
  public Span querySpan() {
    return query_span;
  }

}

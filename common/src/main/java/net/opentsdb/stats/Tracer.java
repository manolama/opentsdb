package net.opentsdb.stats;

import net.opentsdb.stats.Span.SpanBuilder;

public interface Tracer {

  public SpanBuilder newSpan(String id);
  public boolean isDebug();
}

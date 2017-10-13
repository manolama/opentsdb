package net.opentsdb.stats;

public interface Span {
  public void finish();
  public void finish(long duration);
  public void setTag(String key, String value);
  public void setTag(String key, Number value);
  
  public interface SpanBuilder {
    public SpanBuilder asChildOf(Span parent);
    public SpanBuilder withTag(String key, String value);
    public SpanBuilder withTag(String key, Number value);
    public SpanBuilder buildSpan(String id);
    public Span start();
  }
}

package net.opentsdb.query;

public class QueryUpstreamException extends RuntimeException {
  /** Serial for this exception. Auto generated. */
  private static final long serialVersionUID = 7891052172155131854L;

  public QueryUpstreamException(final String msg) {
    super(msg);
  }
  
  public QueryUpstreamException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
}

package net.opentsdb.query;

public class QueryDownstreamException extends RuntimeException {
  /** Serial for this exception. Auto generated. */
  private static final long serialVersionUID = 7891052172155131854L;

  public QueryDownstreamException(final String msg) {
    super(msg);
  }
  
  public QueryDownstreamException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
}

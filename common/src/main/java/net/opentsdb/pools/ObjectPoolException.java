package net.opentsdb.pools;

public class ObjectPoolException extends RuntimeException {
  private static final long serialVersionUID = -7872368672672504583L;

  public ObjectPoolException(final String message) {
    super(message);
  }
  
  public ObjectPoolException(final String message, final Throwable t) {
    super(message, t);
  }
  
  public ObjectPoolException(final Throwable t) {
    super(t);
  }
  
}

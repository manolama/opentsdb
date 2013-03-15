package net.opentsdb.meta;

import java.util.NoSuchElementException;

/**
 * Exception used when a specific annotation cannot be found
 * 
 * @see Annotation
 */
public class NoSuchAnnotation extends NoSuchElementException{

  /** The tsuid requested */
  private final String tsuid;
  
  /** The start timestamp requested */
  private final long start;
  
  public NoSuchAnnotation(final String tsuid, final long start) {
    this.tsuid = tsuid;
    this.start = start;
  }
  
  public String tsuid() {
    return this.tsuid;
  }
  
  public long start() {
    return this.start();
  }
  
  static final long serialVersionUID = 3715776216054583543L;
}

package net.opentsdb.storage;

/**
 * A generic scanner class that storage plugins must implement. Since we deal
 * primarily with BigTable like storage, a request for a huge number of data
 * points should be broken up into "pages" so that we aren't waiting for
 * a giant dataset. 
 * 
 * The only caller configurable field here is the max_values that determines
 * how many objects the caller wants to deal with at a time.
 * 
 * Callers must use the hasNext() method to see if the scanner has found data
 * or reached the end of it's life.
 */
public abstract class Scanner {
  
  /** Represents the last value fetched */
  protected byte[] last;
  
  /** Represents the maximum number of values to fetch per all */
  protected int max_values;
  
  /**
   * Default constructor, initializes objects to null and max_values to 4096
   */
  public Scanner(){
    last = null;
    max_values = 4096;
  }
  
  /**
   * Overload setting the objects to null
   * @param max_values The maximum number of values to fetch
   */
  public Scanner(final int max_values){
    last = null;
    this.max_values = max_values;
  }
  
  /**
   * Used by the caller to determine if there is more data to fetch
   * @return True if there is, false if there isn't any more data
   */
  public abstract boolean hasNext();
}

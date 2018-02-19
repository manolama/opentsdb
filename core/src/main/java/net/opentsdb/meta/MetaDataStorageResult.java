package net.opentsdb.meta;

public interface MetaDataStorageResult {
  public static enum MetaResult {
    DATA,
    NO_DATA_FALLBACK,
    NO_DATA,
    EXCEPTION_FALLBACK,
    EXCEPTION
  }
  
  public MetaResult result();
  
  public Throwable exception();
}

package net.opentsdb.query;

public interface QueryConfig {

  public String getString(final String key, final String default_value);
  
  public int getInt(final String key, final int default_value);
  
  public long getLong(final String key, final long default_value);
  
  public boolean getBoolean(final String key, final boolean default_value);
  
}

package net.opentsdb.config;

import net.opentsdb.core.TSDBPlugin;

public interface Config extends TSDBPlugin {

  public void register(final String property, final String default_value);
  
  
  
}

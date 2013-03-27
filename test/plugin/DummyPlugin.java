package net.opentsdb.plugin;

import org.junit.Ignore;

@Ignore
public abstract class DummyPlugin {
  public String myname;
  
  public DummyPlugin() {
    myname = "";
  }
  
  public abstract String mustImplement();
}

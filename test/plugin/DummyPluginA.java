package net.opentsdb.plugin;

import org.junit.Ignore;

@Ignore
public class DummyPluginA extends DummyPlugin {
  
  public DummyPluginA() {
    this.myname = "Dummy Plugin A";
  }
  
  public String mustImplement() { 
    return this.myname;
  }
}

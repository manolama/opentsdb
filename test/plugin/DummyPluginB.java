package net.opentsdb.plugin;

import org.junit.Ignore;

@Ignore
public class DummyPluginB extends DummyPlugin {
  
  public DummyPluginB() {
    this.myname = "Dummy Plugin B";
  }
  
  public String mustImplement() { 
    return this.myname;
  }
}

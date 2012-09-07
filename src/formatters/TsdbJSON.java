package net.opentsdb.formatters;

import net.opentsdb.core.JSON;

public class TsdbJSON extends FormatterBase {

  public String getOutput(){
    JSON json = new JSON(this.timeseries);
    return json.getJsonString();
  }
}

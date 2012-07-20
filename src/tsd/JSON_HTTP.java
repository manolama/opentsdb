// This file is part of OpenTSDB.
// Copyright (C) 2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import net.opentsdb.core.JSON;

/**
 * This class will help with all JSON related behavior such as parsing or
 * creating JSON strings. It's to help cut down on code repetition such as
 * catching Jackson errors, etc.
 * NOTE: this class is not thread safe, so only use it in the same thread
 * that where you create it.
 */
final class JSON_HTTP extends JSON {
  /**
   * Default constructor
   */
  public JSON_HTTP() {
  }

  /**
   * Constructor that assigns the local object with whatever you pass as a
   * parameter
   * @param o An object to serialize or the type to deserialize
   */
  public JSON_HTTP(Object o) {
    object = o;
  }
  /**
   * Returns a JSON formatted string of the Object provided in the constructor.
   * The JSON string is enclosed in a Javascript style function call for
   * cross-site scripting purposes.
   * @param function
   * @return A Javascript formatted string if successful, an empty string if
   *         there was an error. Check {@link getError} to find out what went
   *         wrong
   */
  public String getJsonPString(final String function) {
    error = "";
    String json = getJsonString();
    if (json.isEmpty())
      return "";
    return (function.length() > 0 ? function : "parseTSDResponse") + "(" + json
        + ");";
  }

  /**
   * Checks the HttpQuery to see if the json=functionName exists and if
   * so then it returns the function name. If not, it returns an empty string
   * @param query The HttpQuery to parse for "json"
   * @return The function name supplied by the user or an empty string
   */
  public static final String getJsonPFunction(final HttpQuery query){
    if (query.hasQueryStringParam("json"))
      return query.getQueryStringParam("json");
    else
      return "";
  }
  
  /**
   * Checks the HttpQuery to see if the query includes "json" and the user
   * wants to get a JSON formatted response
   * @param query The HttpQuery to parse for "json"
   * @return True if the parameter was included, false if not
   */
  public static final boolean getJsonRequested(final HttpQuery query){
    return query.hasQueryStringParam("json");
  }

}

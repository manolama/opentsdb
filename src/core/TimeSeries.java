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
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;

/**
 * This guy defines what composes a time series of data for use in formatters
 */
public class TimeSeries {
  public String aggregator;
  public String metric_name;
  public Map<String, String> tags = null;
  public SortedMap<Long, Object> dps = null;
  public ArrayList<String> uids = null;
  
  public static Boolean isInteger(Object dp){
    if (dp.getClass().equals(Integer.class) || 
        dp.getClass().equals(Long.class) ||
        dp.getClass().equals(Short.class))
      return true;
    else
      return false;
  }
  
  public static Boolean isFloat(Object dp){
    if (dp.getClass().equals(Float.class) || 
        dp.getClass().equals(long.class))
      return true;
    else
      return false;
  }
}

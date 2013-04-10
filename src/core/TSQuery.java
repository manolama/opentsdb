// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.HashMap;

import net.opentsdb.utils.DateTime;

public final class TSQuery {

  /** User given start date/time, could be relative or absolute */
  private String start;
  
  /** User given end date/time, could be relative, absolute or empty */
  private String end;
  
  /** User's timezone used for converting absolute human readable dates */
  private String timezone;
  
  /** Options for serializers, graphs, etc */
  private HashMap<String, ArrayList<String>> options;
  
  /** 
   * Whether or not to include padding, i.e. data to either side of the start/
   * end dates
   */
  private boolean padding;
  
  /** Whether or not to return local annotations */
  private boolean return_annotations;
  
  /** Whether or not to return global annotations, takes longer */
  private boolean return_global_annotations;

  private ArrayList<TSSubQuery> queries;

  // parsed values
  private long start_time;
  private long end_time;
  
  /**
   * Runs through query parameters to make sure it's a valid request.
   * This includes parsing relative timestamps, verifying that the end time is
   * later than the start time (or isn't set), that one or more metrics or
   * tsuids are present, etc. Call this before passing it on for processing.
   * @throws IllegalArgumentException if something is wrong with the query
   */
  public final void validateAndSetQuery() {
    if (this.start == null || this.start.isEmpty()) {
      throw new IllegalArgumentException("Missing start time");
    }
    this.start_time = DateTime.parseDateTimeString(this.start, this.timezone);
    
    if (this.end != null && !this.end.isEmpty()) {
      this.end_time = DateTime.parseDateTimeString(this.end, this.timezone);
    } else {
      this.end_time = System.currentTimeMillis();
    }
    if (this.end_time <= this.start_time) {
      throw new IllegalArgumentException(
          "End time must be greater than the start time");
    }
    
    if (this.queries == null || this.queries.size() < 1) {
      throw new IllegalArgumentException("Missing timeseries queries");
    }
  }
}

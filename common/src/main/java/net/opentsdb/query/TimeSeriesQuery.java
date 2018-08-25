// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query;

import java.util.List;

import com.google.common.hash.HashCode;

import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.filter.NamedFilter;
import net.opentsdb.query.filter.QueryFilter;

/**
 * The base interface for OpenTSDB queries.
 * TODO - this'll need a lot of work.
 * 
 * @since 3.0
 */
public interface TimeSeriesQuery extends Comparable<TimeSeriesQuery> {

  public String getStart();
  
  public String getEnd();
  
  public String getTimezone();
  
  public QueryMode getMode();
  
  public List<NamedFilter> getFilters();

  public QueryFilter getFilter(final String filter_id);
  
  public ExecutionGraph getExecutionGraph();
  
  public TimeStamp startTime();
  
  public TimeStamp endTime();
  
  /** @return A HashCode object for deterministic, non-secure hashing */
  public HashCode buildHashCode();
  
}

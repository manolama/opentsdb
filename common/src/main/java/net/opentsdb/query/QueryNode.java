// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query;

/**
 * TODO 
 * 
 * @since 3.0
 */
public interface QueryNode {

  public QueryPipelineContext context();
  
  public void initialize();
  
  public void fetchNext();
  
  public QueryNodeConfig config();
  
//  /**
//   * Returns a clone of all downstream components for multi-pass operations.
//   * @param listener A non-null listener to use as the sink for the clone.
//   * @param cache Whether or not the downstream clone should cache it's results.
//   * @return A cloned downstream pipeline.
//   */
//  public QueryNode getMultiPassClone(final QueryListener listener, final boolean cache);
  
  public String id();
  
  /**
   * Closes the pipeline and releases all resources.
   */
  public void close();
  
  public void onComplete(final QueryNode downstream, final int final_sequence);
  
  public void onNext(final QueryResult next);
  
  public void onError(final Throwable t);
}

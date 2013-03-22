// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
package net.opentsdb.search;

/**
 * A simple search query to fetch information from the search plugin 
 * implementation. It's not meant to handle every possible type of query into
 * every search implementation, rather it's simply to provide basic search 
 * functionality from the TSD API. 
 * 
 * Search plugin implementations may overload this class as needed.
 * @since 2.0
 */
public class SearchQuery {

  /** A plain-text query that is usually handled by some kind of query parser */
  protected String query;
  
  /** An upper limit to the number of results returned at one time, used for
   * pagination */
  protected int limit;
  
  /** The index of the first record to return, used for pagination */
  protected int start_index;
  
  /** Whether or not to return UIDs only, if querying for TSMeta or UIDMeta */ 
  protected boolean return_uids_only;
  
  /** Whether or not to simply return the list of unique terms for a field 
   * type, such as the unique metrics, tagks or tagvs 
   */
  protected boolean return_terms;
  
  /** Total number of records returned from the plugin */
  protected int total_hits;
  
  /** TODO Type of object we're querying for, TSMeta, UIDMeta, Annotations */
  protected Object type;
  
  /** TODO */
  protected Object results;
}

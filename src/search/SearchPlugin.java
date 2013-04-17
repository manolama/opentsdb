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
package net.opentsdb.search;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import com.stumbleupon.async.Deferred;

/**
 * Search plugins allow data from OpenTSDB to be published to a search indexer.
 * Many great products already exist for searching so it doesn't make sense to
 * re-implement an engine within OpenTSDB. Likewise, going directly to the 
 * storage system for searching isn't efficient. 
 * <p>
 * <b>Note:</b> Implementations must have a parameterless constructor. The 
 * {@link #initialize()} method will be called immediately after the plugin is
 * instantiated and before any other methods are called.
 * <p>
 * <b>Note:</b> Since canonical information is stored in the underlying OpenTSDB 
 * database, the same document may be re-indexed more than once. This may happen
 * if someone runs a full re-indexing thread to make sure the search engine is
 * up to date, particularly after a TSD crash where some data may not have been
 * sent. Be sure to account for that when indexing. Each object has a way to 
 * uniquely identify it, see the method notes below.
 * <p>
 * <b>Warning:</b> All indexing methods should be performed asynchronously. You 
 * may want to create a queue in the implementation to store data until you can 
 * ship it off to the service. Every indexing method should return as quickly as 
 * possible.
 * @since 2.0
 */
public abstract class SearchPlugin {

  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads
   * @param tsdb The parent TSDB object
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}). If initialization fails, call an
   * error in the deferred and the TSD will cease so the user can fix issues.
   */
  public abstract Deferred<Object> initialize(final TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close 
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Indexes a timeseries metadata object in the search engine
   * <b>Note:</b> Unique Document ID = TSUID 
   * @param meta The TSMeta to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexTSMeta(final TSMeta meta);
  
  /**
   * Called when we need to remove a timeseries meta object from the engine
   * <b>Note:</b> Unique Document ID = TSUID 
   * @param tsuid The hex encoded TSUID to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteTSMeta(final String tsuid);
  
  /**
   * Indexes a UID metadata object for a metric, tagk or tagv
   * <b>Note:</b> Unique Document ID = UID and the Type "TYPEUID"
   * @param meta The UIDMeta to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexUIDMeta(final UIDMeta meta);

  /**
   * Called when we need to remove a UID meta object from the engine
   * <b>Note:</b> Unique Document ID = UID and the Type "TYPEUID"
   * @param meta The UIDMeta to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteUIDMeta(final UIDMeta meta);
}

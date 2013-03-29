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
package net.opentsdb.tsd;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;

/**
 * Implementation of the base formatter class. Since all base format calls
 * are for JSON, this class will only overload the required abstract classes
 * and do nothing else.
 * <p>
 * <b>WARNING:</b> Any changes to the JSON formatter should be performed in the
 * {@see HttpFormatter} class since that's where the implementation lives. But
 * beware of versioning when making changes.
 * @since 2.0
 */
final class JsonFormatter extends HttpFormatter {

  /**
   * Default constructor necessary for plugin implementation
   */
  public JsonFormatter() {
    super();
  }
  
  /**
   * Constructor that sets the query object
   * @param query Request/resposne object
   */
  public JsonFormatter(final HttpQuery query) {
    super(query);
  }
  
  /** Initializer, nothing to do for the JSON formatter */
  @Override
  public final void initialize(final TSDB tsdb) {
    // nothing to see here
  }
  
  /** Nothing to do on shutdown */
  public final Deferred<Object> Shutdown() {
    return new Deferred<Object>();
  }
  /** @return the version */
  @Override
  public final String version() {
    return "1.0.0";
  }

  /** @return the shortname */
  @Override
  public final String shortName() {
    return "json";
  }
}

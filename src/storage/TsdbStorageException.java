// This file is part of OpenTSDB.
// Copyright (C) 2011  The OpenTSDB Authors.
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
package net.opentsdb.storage;

public class TsdbStorageException extends RuntimeException {

  /**
   * SerialVersionUID for use in serialization
   */
  private static final long serialVersionUID = 3152557171329889760L;

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  TsdbStorageException(final String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   */
  TsdbStorageException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
  
}

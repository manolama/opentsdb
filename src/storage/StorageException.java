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
package net.opentsdb.storage;

/**
 * Exception class for use by all {@link DataStore} implementations to wrap up
 * storage specific exceptions so that OpenTSDB can handle them properly.
 */
public class StorageException extends RuntimeException {

  /**
   * Default constructor
   * @param msg The message of the exception
   */
  public StorageException(final String msg) {
    super(msg);
  }
  
  /**
   * Constructor wrapping up another exception
   * Note that you can pass standard Java exceptions, but if your storage 
   * implementation has specific exceptions, you need to wrap them in a 
   * StorageException
   * @param msg The message of the exception
   * @param cause The actual exception that was thrown
   */
  public StorageException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
  
  private static final long serialVersionUID = 1746164949299572609L;
}

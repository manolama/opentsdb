// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
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

/** Constants and defaults used in various places.
 * WARNING: Only edit the defaults at the top of this list. Don't mess around
 * with the constants at the end or you will completely screw up your HBase
 * tables. 
 */
public final class Const {
  // DEFAULTS ----------------------------------------------------------------
  // These are operational defaults that can be overridden by the command line
  // or a configuration file. If you're adding a feature to OpenTSDB, please
  // set a default here and use the Configuration class to allow users to
  // configure your settings

  /** Maximum number of tags allowed per data point. */
  public static final short MAX_NUM_TAGS = 8;
  // 8 is an aggressive limit on purpose. Can always be increased later.

  /** Determines the maximum chunk size to accept for the HTTP chunk aggregator */
  public static final int CHUNK_SIZE = 1048576;

  /** How often, in milliseconds, to flush incoming metrics to HBase */
  public static final short FLUSH_INTERVAL = 1000;

  /** The name of the HBase table metric values are stored in */
  public static final String HBASE_TABLE = "tsdb";

  /** The name of the HBase table where metadata is stored */
  public static final String HBASE_UIDTABLE = "tsdb-uid";

  /** The default network port */
  public static final int NETWORK_PORT = 4242;

  /** Enables TCP No delay on the socket */
  public static final boolean NETWORK_TCP_NODELAY = true;

  /** Enables TCP keepalives on the socket */
  public static final boolean NETWORK_KEEPALIVE = true;

  /** Enables reuse of the socket */
  public static final boolean NETWORK_REUSEADDRESS = true;

  /** Defaults to the localhost for connecting to Zookeeper */
  public static final String ZKQUORUM = "localhost";

  /** Disables automatic metric creation */
  public static final boolean AUTOMETRIC = false;
  
  /** Amount of RAM to use for the HTTP cache in KB (default = 32MB) */
  public static final int HTTP_CACHE_RAM_LIMIT = 31250;
  
  /** Largest size data can be to fit in RAM cache in KB */
  public static final int HTTP_CACHE_RAM_FILE_LIMIT = 64; 

  // ************************************************************
  // ************************ WARNING ***************************
  // The constants below determine how metrics are stored in Hbase.
  // If you change any of these constants, you will be unable to access
  // data stored in the TSDB tables and any new data will be unaccessible
  // to other TSD instances.

  /** Number of bytes on which a timestamp is encoded. */
  public static final short TIMESTAMP_BYTES = 4;

  /** Number of LSBs in time_deltas reserved for flags. */
  static final short FLAG_BITS = 4;

  /**
   * When this bit is set, the value is a floating point value. Otherwise it's
   * an integer value.
   */
  static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier. */
  static final short LENGTH_MASK = 0x7;

  /** Mask to select all the FLAG_BITS. */
  static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;

  /** Max time delta (in seconds) we can store in a column qualifier. */
  public static final short MAX_TIMESPAN = 3600;

  /**
   * Array containing the hexadecimal characters (0 to 9, A to F). This array is
   * read-only, changing its contents leads to an undefined behavior.
   */
  public static final byte[] HEX = { '0', '1', '2', '3', '4', '5', '6', '7',
      '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
}

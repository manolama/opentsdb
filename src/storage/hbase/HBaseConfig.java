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
package net.opentsdb.storage.hbase;

import java.util.Map;

import net.opentsdb.utils.Config;

/**
 * Extension of the OpenTSDB config class with defaults for the HBase datastore
 * configured
 */
final class HBaseConfig extends Config {

  /** Whether or not to use zookeeper locking */
  private boolean use_zk_locks;
  
  /** The root path for zookeeper locks */
  private String zk_lock_path;
  
  /**
   * Default constructor, copies values from the parent
   * @param parent The parent config object
   */
  public HBaseConfig(Config parent) {
    super(parent);
  }

  /**
   * Overridden default method
   * Sets defaults for the HBase data store
   */
  @Override
  protected void setDefaults() {
    default_map.put("tsd.storage.hbase.use_zk_locks", "False");
    default_map.put("tsd.storage.hbase.zk_lock_path", "/opentsdb");
    
    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!properties.containsKey(entry.getKey()))
        properties.put(entry.getKey(), entry.getValue());
    }
    
    this.use_zk_locks = this.getBoolean("tsd.storage.hbase.use_zk_locks");
    this.zk_lock_path = this.getString("tsd.storage.hbase.zk_lock_path");
  }
}

// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;

/**
 * The interface for a time series data store factory to spawn instances
 * of a data store.
 * 
 * @since 3.0
 */
public abstract class Tsdb1xDataStoreFactory implements TSDBPlugin {

  /** A TSD to pull config data from. */
  protected TSDB tsdb;
  
  protected String type;
  
  protected String id;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? type : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }
  
  /**
   * Returns a new (or shared) instance of the data store. 
   * @param id An optional (may be null or empty) ID for the instance.
   * @param schema A non-null schema to use for encoding and decoding
   * data with the store.
   * @return A non-null data store.
   */
  public abstract Tsdb1xDataStore newInstance(final String id, 
                                              final Schema schema);
  
  
  @Override
  public String type() {
    return type;
  }
  
  @Override
  public String version() {
    return "3.0.0";
  }
  
  @Override
  public String id() {
    return id;
  }
  
  /** @return Package private TSDB instance to read the config. */
  public TSDB tsdb() {
    return tsdb;
  }
  
}

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
package net.opentsdb.storage;

import java.time.temporal.TemporalAmount;
import java.util.List;

import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.query.QueryNodeConfig;

/**
 * Simple little factory that returns a {@link MockDataStore}.
 * 
 * @since 3.0
 */
public class MockDataStoreFactory extends BaseTSDBPlugin 
                                  implements TimeSeriesDataStoreFactory {

  /** The data store. */
  private volatile MockDataStore mds;
  
  @Override
  public ReadableTimeSeriesDataStore newInstance(final TSDB tsdb, final String id) {
    // DCLP for the singleton.
    if (mds == null) {
      synchronized (this) {
        if (mds == null) {
          mds = new MockDataStore(tsdb, id);
        }
      }
    }
    
    return mds;
  }
 
  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }
  
  @Override
  public String id() {
    return "MockDataStoreFactory";
  }

  @Override
  public String version() {
    // TODO Implement
    return "3.0.0";
  }

  @Override
  public boolean supportsPushdown(Class<? extends QueryNodeConfig> function) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsType(TypeToken<? extends TimeSeriesDataType> type) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<TemporalAmount> durations() {
    // TODO Auto-generated method stub
    return null;
  }
}

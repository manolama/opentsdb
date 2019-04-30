// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import net.opentsdb.data.PartialTimeSeries;
import net.opentsdb.data.PartialTimeSeriesSet;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.pools.CloseablePooledObject;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.rollup.RollupInterval;

public interface Tsdb1xPartialTimeSeries extends PartialTimeSeries, 
    CloseablePooledObject {

  public void reset(final TimeStamp base_timestamp, 
      final long id_hash, 
      final ObjectPool long_array_pool,
      final PartialTimeSeriesSet set,
      final RollupInterval interval);
  
  /**
   * Sorts, de-duplicates and optionally reverses the data in this series. Call
   * it only after adding all of the data.
   * @param keep_earliest Whether or not to keep the earliest duplicates in the
   * array or the latest. 
   * @param reverse Whether or not to reverse the data.
   */
  public void dedupe(final boolean keep_earliest, final boolean reverse);
  
  public void setEmpty(final PartialTimeSeriesSet set);
  
  public void addColumn(final byte prefix,
                        final byte[] qualifier, 
                        final byte[] value);

  public boolean sameHash(final long hash);
}

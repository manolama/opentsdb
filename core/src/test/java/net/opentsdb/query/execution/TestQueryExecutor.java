// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import org.junit.Ignore;

import net.opentsdb.exceptions.QueryExecutionCanceled;
import net.opentsdb.query.AbstractQueryPipeline;
import net.opentsdb.query.QueryListener;
import net.opentsdb.query.QueryPipeline;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.pojo.TimeSeriesQuery;

@Ignore
public class TestQueryExecutor {

  /** Simple implementation to peek into the cancel call. */
  public static class MockDownstream<T> extends QueryExecution<T> {
    public boolean cancelled;
    
    public MockDownstream(TimeSeriesQuery query) {
      super(query);
    }

    @Override
    public void cancel() {
      if (!completed()) {
        cancelled = true;
        callback(new QueryExecutionCanceled("Cancelled!", 0));
      }
    }
    
  }
  
  public static class MockPipeline extends AbstractQueryPipeline {
    public int fetched_next = 0;
    public int closed = 0;
    
    @Override
    public void fetchNext(final int parallel_id) {
      fetched_next++;
    }

    @Override
    public QueryPipeline getMultiPassClone(final QueryListener listener,
                                           final boolean cache) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      if (downstream != null) {
        downstream.close();
      }
      listener.onComplete();
      closed++;
    }
    
  }
  
}

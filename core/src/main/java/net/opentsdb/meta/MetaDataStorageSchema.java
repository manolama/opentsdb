package net.opentsdb.meta;

import net.opentsdb.query.TimeSeriesQuery;

public interface MetaDataStorageSchema {

  public MetaDataStorageResult runQuery(final TimeSeriesQuery query);
}

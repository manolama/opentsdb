package net.opentsdb.search;

import net.opentsdb.core.Annotation;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.TimeSeriesMeta;
import net.opentsdb.search.SearchQuery.SearchResults;

import com.stumbleupon.async.Deferred;

public abstract class Search {

  
  public abstract Deferred<Boolean> indexTimeSeriesMeta(final TimeSeriesMeta meta);
  
  public abstract Deferred<Boolean> indexAnnotation(final Annotation note);
  
  public abstract void reindexTSUIDs(final TSDB tsdb);
  
  public abstract SearchResults searchTSMeta(final SearchQuery query);
}

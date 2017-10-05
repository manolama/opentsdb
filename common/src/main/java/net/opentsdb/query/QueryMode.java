package net.opentsdb.query;

public enum QueryMode {
  SINGLE,             /** All in one. Tight limits. */ 
  CLIENT_STREAM,      /** Client is responsible for requesting the next chunk. */
  CLIENT_STREAM_PARALLEL,  /** Client is responsible for requesting the next chunk but if a query is split (e.g. on metric) then chunks are returned asynchronously.. */
  SERVER_SYNC_STREAM, /** Server will auto push AFTER the current chunk is done. */
  SERVER_SYNC_STREAM_PARALLEL, /** Server will auto push AFTER the current chunk is done but within a chunk it will push asynchronously in parallel. */
  SERVER_ASYNC_STREAM /** Server will push as fast as it can in parallel. */
}

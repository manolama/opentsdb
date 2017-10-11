package net.opentsdb.query;

public enum QueryMode {
  SINGLE,             /** All in one. Tight limits. */ 
  CLIENT_STREAM,      /** Client is responsible for requesting the next chunk. */
  SERVER_SYNC_STREAM, /** Server will auto push AFTER the current chunk is done. */
  SERVER_ASYNC_STREAM /** Server will push as fast as it can in parallel. */
}

package net.opentsdb.storage;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.Tree;
import net.opentsdb.meta.TreeBranch;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueId.UIDType;
import net.opentsdb.utils.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Data Abstraction Layer class for OpenTSDB
 * 
 * Each implementation must: 
 * - Provide a management thread to handle storage issues such as compaction 
 * - Provide asynchronous implementations. Let the caller decide if they want 
 * to wait for data 
 * - Implement shutdown() so that queues can be flushed and storage clients 
 * closed gracefully 
 * - Implement UIDs of some sort for metrics, tag names and values
 */
public abstract class DataStore extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(DataStore.class);

  /** Unique ID cache for the metric names. */
  protected final UniqueId metrics;
  
  /** Unique ID cache for the tag names. */
  protected final UniqueId tag_names;
  
  /** Unique ID cache for the tag values. */
  protected final UniqueId tag_values;
  
  protected final boolean enable_ms_timestamps;
  
  /**
   * Constructor
   * @param config A configuration to use for this data store
   * 
   * @note Implementations will likely want to create their own config 
   * implementation based on this parent
   */
  public DataStore(final Config config){
    metrics = null;
    tag_names = null;
    tag_values = null;
    this.enable_ms_timestamps = 
      config.getBoolean("tsd.core.enable_ms_timestamps");
  }
  
  /**
   * Runs storage management tasks
   * @throws StorageException if the management thread could not be started
   */
  public abstract void start();

  /**
   * Used to flush queues and gracefully close clients on TSD shutdown
   * @return A deferred object to wait on before closing the thread
   * @throws StorageException
   */
  public abstract Deferred<Object> shutdown();

  /**
   * For storage systems that batch data, forces a batch send
   * @return A deferred object to wait on if necessary
   * @throws StorageException
   */
  public abstract Deferred<Object> flushData();
  
  /**
   * Flushes the UID maps and can be overridden to flush other caches
   */
  public void flushCaches(){
    if (this.metrics != null)
      this.metrics.dropCaches();
    if (this.tag_names != null)
      this.tag_names.dropCaches();
    if (this.tag_values != null)
      this.tag_values.dropCaches();
  }

  /**
   * Retrieves statistics about the underlying storage system and UID maps
   * @param collector A collector object to update with storage stats
   */
  public abstract void collectStats(final StatsCollector collector);

  /**
   * Fetches the name assigned with a UID, may return from the cache
   * @param type The type of UID to fetch; metric, tagk, tagv
   * @param uid The UID to fetch
   * @return The name associated with the UID
   * @throws StorageException
   * @throws NoSuchUniqueName
   * @throws IllegalArgumentException
   */
  public abstract Deferred<String> getName(final UIDType type, 
      final byte[] uid);

  /**
   * Fetches the UID associated with a name
   * @param type The type of UID to fetch; metric, tagk, tagv
   * @param name The name of the object to match on
   * @return The UID as a byte array
   * @throws StorageException
   * @throws NoSuchUniqueId
   * @throws IllegalArgumentException
   */
  public abstract Deferred<byte[]> getUID(final UIDType type, 
      final String name);

  /**
   * Attempts to fetch the UID associated with a name, or creates a new one 
   * if not found
   * @param type The type of UID to fetch; metric, tagk, tagv
   * @param name The name of the object to match on
   * @return The UID as a byte array
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<byte[]> getOrAssignUID(final UIDType type,
      final String name);

  /**
   * Attempts to rename the given UID and update associated metadata entries
   * @param type The type of the UID to fetch; metric, tagk, tagv
   * @param current_name The current name of the UID
   * @param new_name The new name of the UID
   * @return An object to wait on optionally
   * @throws StorageException
   * @throws IllegalArgumentException if data is missing OR the new name
   * already has an entry
   */
  public abstract Deferred<Object> renameUID(final UIDType type, 
      final String current_name, final String new_name);
  
  /**
   * Scans the UID table for errors such as orphaned UID names or incorrect
   * mappings
   * 
   * Implementations should check for the following:
   * - Validate UID forward and reverse mapping
   * - Validate cache entries exist in the table
   * - Validate UIDMeta entries associated with UIDs
   * - Validate TSMeta objects have associated UIDs
   * 
   * If fixing:
   * - Attempt to repair UID mappings @todo how?
   * - Purge errant cache entries
   * - Add UIDMeta entries that are missing
   * 
   * @note This method should return periodically to update the caller on 
   * progress
   * 
   * @param fix Whether or not to fix detected errors. If false, a report
   * should be returned
   * @param query Scanner used to keep track of progress
   * @return @todo Define a return object/type. Maybe just an array of 
   * messages?
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Object> fsckUIDs(final boolean fix, 
      StorageQuery query);
  
  /**
   * Performs a regex match against the given UID type and returns matches
   * @param type The type of the UID to fetch; metric, tagk, tagv
   * @param regex_query A valid regex string that must compile
   * @return A map of string to UIDs
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws PatternSyntaxException
   */
  public abstract Deferred<Map<String, byte[]>> grepUIDs(final UIDType type, 
      final String regex_query);
  
  /**
   * Attempts to delete a UID and any associated data points and metadata
   * 
   * @warn This method should only be called from CLI tools as it can take
   * a long time to find all associated data and delete it
   * 
   * @note Implementers should follow these steps:
   * - Delete the UID mapping in storage
   * - Delete the cache in this object
   * - Delete any associated UIDMeta entries
   * - Scan for associated timeseries
   * - Delete TSMeta entries
   * - Scan and delete data points
   * 
   * If someone tries to write a new data point with the deleted UID, it will
   * create a new UID. If the TSD crashes, the fsck call can be used to clean
   * up
   * 
   * @param type The type of the UID to fetch; metric, tagk, tagv
   * @param uids A list of UID names to map to values and delete
   * @param query Scanner to keep track of progress
   * @return @todo Define
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Object> deleteUIDs(final UIDType type, 
      final String[] uids, StorageQuery query);
  
  /**
   * Attempts to delete the TSUIDs and associated data points and metadata
   * 
   * @warn This method should only be called from CLI tools as it can take
   * a long time to find all associated data and delete it
   * 
   * @note Implementers should follow these steps:
   * - Delete the TSMeta entries
   * - Scan and delete data points
   * 
   * @param tsuids List of TSUIDs as hexadecimal values
   * @param query Scanner to keep track of progress
   * @return @todo Define
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Object> deleteTSUIDs(final String[] tsuids,
      StorageQuery query);
  
  /**
   * Searches for timeseries data points that match the query
   * 
   * @note The implementation should do its best to split the query into 
   * simultaneous searches, scans, etc so that the call can complete as 
   * quickly as possible.
   * 
   * @warn At this time, any query results must fit in RAM
   * 
   *       NOTES: In this case, Span would be the same as the existing HBase
   *       schema. The storage system would have to merge the data points into
   *       the same qualifier/timestamp offset byte array and values would be
   *       stored in a byte array. This is so the grouping and aggregation code
   *       can do it's thing
   * 
   * @todo Extend the TsdbQuery object a lot
   *       
   * The output can be parsed through aggregators or dumped to a file, whatever
   * a formatter declares. This can also be used for exporting data 
   * 
   * @param query The TimesereiesQuery that determines what to search for
   * @return A tree map of associated data points to pass on to the aggregators
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<TreeMap<byte[], Span>> queryTSData(
      final TsdbQuery query);

  /**
   * Attempts to delete data matching the requested query
   * 
   * @param query A query defining the types and times of data to purge
   * @return @todo ?
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Object> deleteTSData(
      final TsdbQuery query);
  
  /**
   * Scans the data point store for data formatting issues
   * @param fix Whether or not to fix discovered errors. If false, this method
   * should return a report
   * @param query A query definining the types and times of data to validate
   * @return @todo ?
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Object> fsckTSData(final boolean fix,
      final TsdbQuery query);

  /**
   * Stores a data point or queues it for batch insertion
   * @param metric The name of the metric
   * @param timestamp The Unix Epoch timestamp in seconds or milliseconds
   * @param value A float or integer value to store
   * @param tags Map of tagk/tagvs
   * 
   * @warn This is part of OpenTSDB's critical path so make sure Implementations
   *       are as fast as possible. Implementations will generally ignore the
   *       return from this method. Implementations should mimick the behavior
   *       of the Asyncbase library in attempting to store the DP, then queue it
   *       if it's unable to.
   * 
   * @return True if stored or queued successfully
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Boolean> putDataPoint(final String metric, 
      final long timestamp, final Object value, 
      final Map<String, String> tags);

  /**
   * Attempts to retrieve a specific local or global annotation based on 
   * tsuid and/or start time
   * @param start The start timestamp for the annotation, required
   * @param tsuid If fetching an annotation associated with a timeseries, this
   * must be set. Otherwise if fetching a global meta, this may be null or an
   * empty string
   * @return An annotation object if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchAnnotation
   */
  public abstract Deferred<Annotation> getAnnotation(final long start, 
      final String tsuid);
  
  /**
   * Attempts to delete a list of global or local annotations
   * @param annotations A list of annotations to delete
   * @return True if completed
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchAnnotation
   */
  public abstract Deferred<Boolean> deleteAnnotations(
      final Map<Long, String> annotations);

  /**
   * Attempts to retrieve one or more annotations based on the query
   * 
   * The annotation query can use a scanner to iterate through the global or
   * local annotations
   * 
   * @param start The start timestamp to search for. May be 0 in which case
   * the implementer should scan the entire data store for data
   * @param end The time to stop scanning for data. Must be great than start
   * @param tsuid Optional TSUID if searching for annotations associated with
   * a specific timeseries. Otherwise global annotations should be searched
   * @param query A scanner object used for iterating through results
   * @return A list of annotation objects if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<List<Annotation>> getAnnotations(final long start,
      final long end, final String tsuid, StorageQuery query);

  /**
   * Attempts to store the given annotation
   * 
   * @warning Existing annotations at the same {@link start_time} will be
   * overwritten
   * @param note The annotation to store
   * @return The given annotation object
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Annotation> putAnnotation(final Annotation note);

  /**
   * Attempts to retrieve the metadata associated with a UID
   * @param type The type of UID to fetch; metric, tagk, tagv
   * @param uid The UID to fetch
   * @return A UIDMetaData object if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchUniqueId
   */
  public abstract Deferred<UIDMeta> getUIDMetaData(final UIDType type,
      final byte[] uid);

  /**
   * Attempts to store the metadata object, overwriting any existing entry
   * @param meta The metadata object to store
   * @return The given metadata object
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<UIDMeta> putUIDMetaData(final UIDMeta meta);

  /**
   * Attempts to retrieve the metadata associated with a timeseries UID
   * @param tsuid The TSUID to fetch metadata for
   * @return A TSMetaData object if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchUniqueId
   */
  public abstract Deferred<TSMeta> getTSMetaData(final byte[] tsuid);

  /**
   * Returns a hash of all unique TSUIDs to store in memory for use in
   * determining when a new TSUID has been ingested. The CompactHashSet stores
   * integer hashes of the TSUIDs to keep heap allocation small and lookups
   * faster. There's may be a better way but this has proven to be pretty fast.
   * @return A CompactHashSet of hashes of the UIDs
   * @throws StorageException
   */
  public abstract Deferred<HashSet<Integer>> getAllTSUIDs();

  /**
   * Allows iteration through the entire TSUID set without loading the entire
   * domain in RAM
   * @param query The state object to use for querying and tracking progress
   * @return A list of TSMetaData objects
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<List<TSMeta>> scanTSUIDs(StorageQuery query);

  /**
   * Attempts to fetch a branch from storage
   * @param tree_id The ID of the tree to retrieve a branch for
   * @param branch_id The ID of a branch to retrieve
   * @return The MetaTreeBranch object if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchTree
   * @throws NoSuchTreeBranch
   */
  public abstract Deferred<TreeBranch> getBranch(final int tree_id,
      final int branch_id);
  
  /**
   * Attempts to delete the branch in storage
   * @param tree_id The ID of the tree to retrieve a branch for
   * @param branch_id The ID of a branch to retrieve
   * @return True if successful
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchTree
   * @throws NoSuchTreeBranch
   */
  public abstract Deferred<Boolean> deleteBranch(final int tree_id,
      final int branch_id);

  /**
   * Attempts to store a branch, overwriting any existing entry
   * @param branch The branch to store
   * @return The same branch object
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchTree
   */
  public abstract Deferred<TreeBranch> putBranch(final TreeBranch branch);

  /**
   * Attempts to retrieve the metadata associated with a metadata tree
   * @param tree_id The ID of the tree to retrieve
   * @return The Tree object if found, null if not
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchTree
   */
  public abstract Deferred<Tree> getTree(final int tree_id);

  /**
   * Attempts to delete a tree and it's branches from storage
   * 
   * @warn The implementation should mark the tree for deletion so that
   * any tree builders performing a reindexing don't keep adding branches
   * 
   * @param tree_id The ID of the tree to delete
   * @return True if successful
   * @throws StorageException
   * @throws IllegalArgumentException
   * @throws NoSuchTree
   */
  public abstract Deferred<Boolean> deleteTree(final int tree_id);
  
  /**
   * Attempts to store the tree, overwriting any existing entry
   * @param tree Tree object to store
   * @return The same tree object
   * @throws StorageException
   * @throws IllegalArgumentException
   */
  public abstract Deferred<Tree> putTree(final Tree tree);

  /**
   * Attempts to retrieve a new tree from storage, allocating a new tree ID
   * @return An empty Tree object, with an assigned tree ID, if successful
   * @throws StorageException
   */
  public abstract Deferred<Tree> getNewTree();
}
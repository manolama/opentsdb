package net.opentsdb.meta;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Annotations are used to record time-based notes about timeseries events.
 * Every note must have an associated {@link start_time} as that determines
 * where the note is stored.
 * 
 * Annotations may be associated with a specific timeseries, in which case
 * the {@link tsuid} must be configured with a valid TSUID. If no TSUID
 * is provided, the annotation is considered a "global" note that applies
 * to everything stored in OpenTSDB.
 * 
 * The {@link description} field should store a very brief line of information
 * about the event. GUIs can display the description in their "main" view
 * where multiple annotations may appear. Users of the GUI could then click
 * or hover over the description for more detail including the {@link notes}
 * field.
 * 
 * Custom data can be stored in the {@link custom} hash map for user
 * specific information. For example, you could add a "reporter" key
 * with the name of the person who recorded the note.
 */
public class Annotation {
  private static final Logger LOG = LoggerFactory.getLogger(Annotation.class);
  
  /** If the note is associated with a timeseries, represents the ID */
  public String tsuid;
  
  /** The start timestamp associated wit this note in seconds or ms */
  public long start_time;
  
  /** Optional end time if the note represents an event that was resolved */
  public long end_time;
  
  /** A short description of the event, displayed in GUIs */
  public String description;  
  
  /** A detailed accounting of the event or note */
  public String notes;
  
  /** Optional user supplied key/values */
  public HashMap<String, String> custom;
}

// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
 * Custom data can be stored in the {@link #custom} hash map for user
 * specific information. For example, you could add a "reporter" key
 * with the name of the person who recorded the note.
 * @since 2.0
 */
public class Annotation {
  private static final Logger LOG = LoggerFactory.getLogger(Annotation.class);
  
  /** If the note is associated with a timeseries, represents the ID */
  private String tsuid;
  
  /** The start timestamp associated wit this note in seconds or ms */
  private long start_time;
  
  /** Optional end time if the note represents an event that was resolved */
  private long end_time;
  
  /** A short description of the event, displayed in GUIs */
  private String description;  
  
  /** A detailed accounting of the event or note */
  private String notes;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom;
}

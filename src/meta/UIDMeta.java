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
 * UIDMeta objects are associated with the {@link #UniqueId} of metrics, tag names
 * or tag values. When a new metric, tagk or tagv is generated, a  UIDMeta object
 * will also be written to storage with only the {@link #uid}, {@link #type} and
 * {@link #name} filled out. Users can then modify mutable fields
 * @since 2.0
 */
public class UIDMeta {
  protected static final Logger LOG = LoggerFactory.getLogger(UIDMeta.class);
  
  /** A hexadecimal representation of the UID this metadata is associated with */
  private String uid;
  
  /** The type of UID this metadata represents */
  private int type;
  
  /** 
   * This is the identical name of what is stored in the UID table
   * It cannot be overridden 
   */
  private String name;
  
  /** 
   * An optional, user supplied name used for display purposes only
   * If this field is empty, the {@link name} field should be used
   */
  private String display_name;
  
  /** A short description of what this object represents */
  private String description;
  
  /** Optional, detailed notes about what the object represents */
  private String notes;
  
  /** A timestamp of when this UID was first recorded by OpenTSDB in seconds */
  private long created;
  
  /** Optional user supplied key/values */
  private HashMap<String, String> custom;
}

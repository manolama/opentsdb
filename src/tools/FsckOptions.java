// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import net.opentsdb.utils.Config;

/**
 * Various options to use during fsck over OpenTSDB tables
 */
final class FsckOptions {
  private boolean fix;
  private boolean compact;
  private boolean last_write_wins;
  private boolean delete_orphans;
  private boolean delete_unknown_columns;
  private boolean delete_bad_values;
  
  /**
   * Default Ctor that sets the options based on command line flags and config
   * object
   * @param argp Command line arguments post parsing
   * @param config The configuration object loaded from a file 
   */
  public FsckOptions(final ArgP argp, final Config config) {
    fix = argp.has("--fix");
    compact = argp.has("--compact");
    last_write_wins = argp.has("--last_write_wins") || 
        config.getBoolean("tsd.storage.fix_duplicates");
    delete_orphans = argp.has("--delete-orphans");
    delete_unknown_columns = argp.has("--delete-unknown-columns");
    delete_bad_values = argp.has("--delete-bad-values");
  }
  
  /**
   * Add data table fsck options to the command line parser
   * @param argp The parser to add options to
   */
  public static void addDataOptions(final ArgP argp) {
    argp.addOption("--fix", "Fix errors as they're found.");
    argp.addOption("--full-table", "Scan the entire data table for errors.");
    argp.addOption("--compact", "Compactions rows matching the query.");
    argp.addOption("--last-write-wins", 
        "Last data point written will be kept when fixing duplicates.");
    argp.addOption("--delete-orphans", 
        "Delete any time series rows where one or more UIDs fail resolution.");
    argp.addOption("--delete-unknown-columns", 
        "Delete any unrecognized column that doesn't belong to OpenTSDB");
    argp.addOption("--delete-bad-values", 
        "Delete single column datapoints with bad values");
  }
  
  /** @return Whether or not to fix errors while processing. Does not affect 
   * compacting */
  public boolean fix() {
    return fix;
  }
  
  /** @return Whether or not to compact rows while processing. Can cause 
   * compaction without the --fix flag. Will skip rows with duplicate data 
   * points unless --last-write-wins is also specified or set in the config 
   * file */
  public boolean compact() {
    return compact;
  }
  
  /** @return Accept data points with the most recent timestamp when duplicates 
   * are found */
  public boolean lastWriteWins() {
    return last_write_wins;
  }
  
  /** @return Whether or not to delete rows where the UIDs failed to resolve 
   * to a name */
  public boolean deleteOrphans() {
    return delete_orphans;
  }
  
  /** @return Delete columns that aren't recognized */
  public boolean deleteUnknownColumns() {
    return delete_unknown_columns;
  }
  
  /** @return Remove data points with bad values */
  public boolean deleteBadValues() {
    return delete_bad_values;
  }

  
  /** @param fix Whether or not to fix errors while processing. Does not affect 
   * compacting */
  public void setFix(final boolean fix) {
    this.fix = fix;
  }
  

  /** @param compact Whether or not to compact rows while processing. Can cause 
   * compaction without the --fix flag. Will skip rows with duplicate data 
   * points unless --last-write-wins is also specified or set in the config 
   * file */
  public void setCompact(final boolean compact) {
    this.compact = compact;
  }
  

  /** @param last_write_wins Accept data points with the most recent timestamp when duplicates 
   * are found */
  public void setLastWriteWins(final boolean last_write_wins) {
    this.last_write_wins = last_write_wins;
  }
  

  /** @param delete_orphans Whether or not to delete rows where the UIDs failed to resolve 
   * to a name */
  public void setDeleteOrphans(final boolean delete_orphans) {
    this.delete_orphans = delete_orphans;
  }
  

  /** @param delete_unknown_columns Delete columns that aren't recognized */
  public void setDeleteUnknownColumns(final boolean delete_unknown_columns) {
    this.delete_unknown_columns = delete_unknown_columns;
  }
  

  /** @param delete_bad_values Remove data points with bad values */
  public void setDeleteBadValues(final boolean delete_bad_values) {
    this.delete_bad_values = delete_bad_values;
  }

}

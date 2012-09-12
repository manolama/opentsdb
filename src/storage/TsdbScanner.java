// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.storage;

/**
 * Implements an abstract Scanner class container that can be passed to storage
 * implementations for iterating through a table.
 * 
 * Abstracting standard storage calls are pretty easy, but scanners are
 * complicated because they need to track state and provide async callbacks.
 * 
 * This generic scanner will store the client's scanner in the "scanner" object.
 * Pass this whole object back to the TsdbStore implementation and pull the
 * "scanner" out for use.
 * 
 */
public class TsdbScanner {
  /** Starting point for the scanner. Defaults to 0 */
  private byte[] start_row = new byte[0];
  /** End point for the scanner. Defaults to 0 */
  private byte[] end_row = new byte[0];
  /** Name of the table to scan if overriding the client's table */
  private byte[] table = null;
  /** Optional column family value */
  private byte[] family = null;
  /** Optional qualifier */
  private byte[] qualifier = null;
  /** Maximum number of key/value pairs to return in each nextRow() call */
  private int max_kvs = 4096;
  /** Maximum number of rows to return for each nextRow() call */
  private int max_rows = 128;
  /** Optional regex to run on the row for searching values */
  private String row_regex;
  /** The client specific scanner object */
  private Object scanner = null;

  /**
   * Default constructor, everything set to its default
   */
  public TsdbScanner() {
  }

  /**
   * Constructor that initializes the start and end rows as well as the table name
   * @param start_row Starting row for the scan
   * @param end_row Stopping row for the scan
   * @param table Name of the table if overriding the client's table. Can pass NULL
   * and the value won't be saved.
   */
  public TsdbScanner(final byte[] start_row, final byte[] end_row,
      final byte[] table) {
    this.start_row = start_row;
    this.end_row = end_row;
    if (table != null)
      this.table = table;
  }

  // GETTERS AND SETTERS -------------------------------------------------
  
  public byte[] getStart_row() {
    return start_row;
  }

  public void setStartRow(byte[] start_row) {
    this.start_row = start_row;
  }

  public byte[] getEndRow() {
    return end_row;
  }

  public void setEndRow(byte[] end_row) {
    this.end_row = end_row;
  }

  public byte[] getTable() {
    return table;
  }

  public void setTable(byte[] table) {
    this.table = table;
  }

  public byte[] getFamily() {
    return family;
  }

  public void setFamily(byte[] family) {
    this.family = family;
  }

  public int getMaxKeyValues() {
    return max_kvs;
  }

  public void setMaxKeyValues(int max_kvs) {
    this.max_kvs = max_kvs;
  }

  public int getMaxRows() {
    return max_rows;
  }

  public void setMaxRows(int max_rows) {
    this.max_rows = max_rows;
  }

  public String getRowRegex() {
    return row_regex;
  }

  public void setRowRegex(String row_regex) {
    this.row_regex = row_regex;
  }

  public Object getScanner() {
    return scanner;
  }

  public void setScanner(Object scanner) {
    this.scanner = scanner;
  }

  public void setQualifier(byte[] qualifier) {
    this.qualifier = qualifier;
  }

  public byte[] getQualifier() {
    return qualifier;
  }
}

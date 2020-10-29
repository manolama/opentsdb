package net.opentsdb.data;

import java.io.Closeable;
import java.time.temporal.ChronoUnit;

public interface LowLevelTimeSeries extends Closeable {

  static enum Format {
    ASCII_STRING,
    UTF8_STRING,
    ENCODED
  }
//  
//  /** Releases pooled resources, etc. */
//  void close();
//  
  /** The raw buffer. */
  byte[] rawBuffer();
  
  /** If there is a next value, call this to advance to that entry. Instead of
   * throwing exceptions we return false if something went wrong. */
  boolean advance();
  
  /** The timestamp and format for the current entry. */
  long timestamp();
  ChronoUnit timeStampFormat();
  
  /** Whether or not the tags are common across entries. Lets the store decide
   * if it needs to read the tags each time or not. */
  boolean commonTagSet();
  
  /** ASSUMPTION: tags have been sorted and are consecutive stored in the buffer
   * in key<delim>value<delim>key<delim>value[...] format. */
  byte[] tagsBuffer();
  Format tagsFormat();

  /** The tag delimiter for use when reading the entire tag set in one go
   * instead of using {@link #advanceTagPair()}. */
  byte tagDelimiter();
  
  /** number of tag key/value pairs. */
  int tagSetCount();
  
  /** ... then if there are more, call this to move to the next. */
  boolean advanceTagPair();

  /** Indices into the buffer when using the {@link hasNextTagPair} method. */
  int tagKeyStart();
  int tagKeyLength();
  
  /** Indices into the buffer when using the {@link hasNextTagPair} method. */
  int tagValueStart();
  int tagValueLength();
  
  /** The index of the end of the tag set in the buffer. */
  int tagSetLength(); 
  
  public interface Namespaced extends LowLevelTimeSeries {
    byte[] namespaceBuffer();
    int namespaceStart();
    int namespaceLength();
  }
}

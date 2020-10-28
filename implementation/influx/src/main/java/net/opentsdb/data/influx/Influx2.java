package net.opentsdb.data.influx;

import java.io.IOException;
import java.io.InputStream;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import net.opentsdb.common.Const;
import net.opentsdb.data.LowLevelMetric.HashedLowLevelMetric;
import net.opentsdb.data.LowLevelTimeSeries.Namespaced;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.Parsing;
import net.opentsdb.utils.XXHash;

public class Influx2 implements HashedLowLevelMetric, Namespaced {
  static int escaped_mask = 0x80000000;
  static int UNescaped_mask = 0x7FFFFFFF;
  static int chunks = 4096;

  byte[] namespace;
  
  byte[] buffer;
  InputStream stream;
  boolean eos;
  int offset;
  int end;
  
  int lineStart;
  int lineEnd;
  
  int readIdx;
  
  /** since there can be multiple "fields" for the "measurement", we need to 
   * smoosh them into a buffer and append a dot, e.g. "measurement"."field". 
   */
  byte[] metric_buffer = new byte[1024];
  int measurementIndex;
  int metricBytes;
  
  long[] fieldIndices = new long[8];
  int fieldIndex;
  long[] valueIndices = new long[8];
  int valueIndex;
  
  byte[] tagBuffer = new byte[26];
  int tagsEnd = 0;
  int tagsCount = 0;
  int tagIndex = 0;
  long tagKey;
  long tagValue;
  
  long timestamp;
  
  long[] long_value = new long[1];
  double[] double_value = new double[1];
  long[] temp_long = new long[1];
  double[] temp_double = new double[1];
  ValueFormat valueFormat;
  
  // Hashes
  boolean hashIt;
  long seriesHash;
  long metricHash;
  long tagSetHash;
  long tagKeyHash;
  long tagValueHash;
  long tagPairHash;
  
  public void setBuffer(final byte[] buffer) {
    this.buffer = buffer;
    offset = 0;
    end = buffer.length;
    //newLineIdx = offset;
    lineStart = offset;
    lineEnd = lineStart;
    namespace = null;
  }
  
  public void setBuffer(byte[] bytes, int offset, int length) {
    buffer = bytes;
    this.offset = offset;
    end = offset + length;
    //newLineIdx = offset;
    lineStart = offset;
    lineEnd = lineStart;
    namespace = null;
  }
  
  public void setInputStream(final InputStream stream) {
    if (buffer == null) {
      buffer = new byte[chunks * 2];
    }
    this.stream = stream;
    eos = false;
    offset = 0;
    end = 0;
    lineStart = 0;
    lineEnd = lineStart;
    namespace = null;
  }
  
  public void setNamespace(final String namespace) {
    this.namespace = namespace.getBytes(Const.UTF8_CHARSET);
  }
  
  @Override
  public Format metricFormat() {
    return Format.UTF8_STRING;
  }

  @Override
  public int metricStart() {
    return 0;
  }

  @Override
  public int metricEnd() {
    return metricBytes;
  }

  @Override
  public byte[] metricBuffer() {
    return metric_buffer;
  }

  @Override
  public ValueFormat valueFormat() {
    return valueFormat;
  }

  @Override
  public long intValue() {
    return long_value[0];
  }

  @Override
  public float floatValue() {
    return (float) double_value[0];
  }

  @Override
  public double doubleValue() {
    return double_value[0];
  }

  @Override
  public void close() {
    if (stream != null) {
      try {
        stream.close();
      } catch (IOException e) {
        // safeish to ignore it.
        e.printStackTrace();
      }
      stream = null;
    }
  }

  @Override
  public byte[] rawBuffer() {
    return buffer;
  }

  @Override
  public boolean advance() {
    // first see if we have more fields to read for the current line.
    if (readIdx < fieldIndex) {
      int start = (int) (fieldIndices[readIdx] >> 32);
      int end = (int) fieldIndices[readIdx];
      appendField(start, end);
      
      start = (int) (valueIndices[readIdx] >> 32);
      end = (int) valueIndices[readIdx];
      parseValue(start, end, true);
      readIdx++;
      tagIndex = 0;
      return true;
    }
    
    // reset
    readIdx = 1;
    tagsEnd = 0;
    
    //System.out.println("______________________________ NEW LINE _______________");
    lineStart = lineEnd > 0 ? lineEnd + 1 : 0;
    
    if (stream != null) {
      return advanceStream();
    }
    return advanceBytes();
//    if (lineStart >= end) {
//      System.out.println("******** past end: " + end);
//      return false;
//    }
//    
//    // consume whitespace to get to the first measurement.
//    while (lineStart < end) {
//      lineStart = findNextChar(lineStart);
//      if (lineStart >= end) {
//        System.out.println("******** past end: " + end);
//        return false;
//      }
//      if (buffer[lineStart] == '#') {
//        // it's a comment;
//        lineEnd = findNextNewLine(lineStart);
//        lineStart = lineEnd;
//        continue;
//      }
//      
//      if (lineStart >= end) {
//        System.out.println("******** past end 2: " + end);
//        return false;
//      }
//      
//      lineEnd = findNextNewLine(lineStart);
//      System.out.println(" lineEnd: " + lineEnd +"    END: " + end);
//
//      //System.out.println("@@@@@ S: " + lineStart + "  -  " + lineEnd);
//      System.out.println(" MATCHED! s " + lineStart + " e: " + lineEnd 
//          + "  [" + new String(buffer, lineStart, lineEnd - lineStart) + "]");
//      if (processLine()) {
//        return true;
//      }
//      System.out.println("----------- FAILED to match");
//
//      // shift and try again
//      System.out.println("DAMN");
//      lineStart = lineEnd;
//    }
//    
//    // TODO - tons of work to validate here.
//    // fell through so nothing left.
//    lineStart = end;
//    return false;
  }

  boolean advanceBytes() {
    System.out.println("______________________________ NEW LINE _______________");
    lineStart = lineEnd > 0 ? lineEnd + 1 : 0;
    
    if (lineStart >= end) {
      System.out.println("******** past end: " + end);
      return false;
    }
    
    // consume whitespace to get to the first measurement.
    while (lineStart < end) {
      lineStart = findNextChar(lineStart);
      if (lineStart >= end) {
        System.out.println("******** past end: " + end);
        return false;
      }
      if (buffer[lineStart] == '#') {
        // it's a comment;
        lineEnd = findNextNewLine(lineStart);
        lineStart = lineEnd;
        continue;
      }
      
      if (lineStart >= end) {
        System.out.println("******** past end 2: " + end);
        return false;
      }
      
      lineEnd = findNextNewLine(lineStart);
//      System.out.println(" lineEnd: " + lineEnd +"    END: " + end);

      //System.out.println("@@@@@ S: " + lineStart + "  -  " + lineEnd);
      
      for (int x = lineStart; x < lineEnd; x++) {
        if (buffer[x] == 0) {
          System.out.println("!!!!!!!!!!!!!!!!!!!!!!! WTF?????? Null at " + x + " in line^^^^^^^^^^");
          System.exit(2);
        }
        System.out.println("               " + x + " was ok.");
      }
      System.out.println(" MATCHED! s " + lineStart + " e: " + lineEnd 
          + "  [" + new String(buffer, lineStart, lineEnd - lineStart) + "]");
      if (processLine()) {
        return true;
      }
      System.out.println("----------- FAILED to match");

      // shift and try again
      System.out.println("DAMN");
      lineStart = lineEnd;
    }
    
    // TODO - tons of work to validate here.
    // fell through so nothing left.
    lineStart = end;
    return false;
  }

  boolean advanceStream() {
    if (eos) {
      return false;
    }
    
    while (!eos) {
      // find the next new line
      int newline = findNextNewLine(lineStart);
      if (newline < 0) {
        // need more data!
        newline = readFromStream();
        if (newline < 0) {
          // all done
          return false;
        }
      }
      
      lineStart = findNextChar(lineStart);
      if (lineStart >= end) {
        continue;
      }
      if (buffer[lineStart] == '#') {
        // it's a comment;
        lineEnd = findNextNewLine(lineStart);
        lineStart = lineEnd;
        continue;
      }
      
      if (lineStart >= end) {
        continue;
      }
      
      lineEnd = newline;
      System.out.println(" lineEnd: " + lineEnd +"    END: " + end);

      //System.out.println("@@@@@ S: " + lineStart + "  -  " + lineEnd);
      for (int x = lineStart; x < lineEnd; x++) {
        if (buffer[x] == 0) {
          System.out.println("!!!!!!!!!!!!!!!!!!!!!!! WTF?????? Null at " + x + " in line^^^^^^^^^^");
          System.exit(2);
        }
      }
      System.out.println(" MATCHED! s " + lineStart + " e: " + lineEnd 
          + "  [" + new String(buffer, lineStart, lineEnd - lineStart) + "]");
      if (processLine()) {
        return true;
      }
      System.out.println("----------- FAILED to match");

      // shift and try again
      System.out.println("DAMN");
      lineStart = lineEnd;
    }
    
    // eos
    return false;
  }
  
  int readFromStream() {
    // to avoid growing the buffer if we don't have to we shift what hasn't
    // been processed.
    if (lineStart > 0) {
      System.arraycopy(buffer, lineEnd + 1, buffer, 0, end - lineEnd);
      
      end = end - lineEnd;
      lineStart = 0;
    }
    
    while (!eos) {
      if (end + chunks >= buffer.length) {
        byte[] temp = new byte[buffer.length * 2];
        System.arraycopy(buffer, 0, temp, 0, end);
        buffer = temp;
        System.out.println("@@@@@@@@@@@@@@ Expanding buffer for stream.");
      }
      
      int read;
      try {
        read = stream.read(buffer, end > 0 ? end - 1 : 0, chunks);
        System.out.println("************ READ: " + read + " and appended to " + end);
        if (read < 0) {
          eos = true;
          return -1;
        }
        end += read - 1;
        
        int newline = findNextNewLine(lineStart);
        //System.out.println("********* ST: " + lineStart + "  NL: " + newline);
        if (newline > 0) {
          return newline;
        }
        // get some more info.
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read from the stream", e);
      }
    }
    if (end > lineStart) {
      //System.out.println("********* Had data: " + end);
      return end;
    }
    return -1;
  }
  
  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public ChronoUnit timeStampFormat() {
    return ChronoUnit.NANOS;
  }

  @Override
  public boolean commonTagSet() {
    return false;
  }

  @Override
  public byte[] tagsBuffer() {
    return tagBuffer;
  }

  @Override
  public Format tagsFormat() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte tagDelimiter() {
    return ' ';
  }

  @Override
  public int tagSetCount() {
    return tagsCount;
  }
  
  @Override
  public boolean advanceTagPair() {
    if (tagIndex >= tagsEnd) {
      return false;
    }
    
    int start = tagIndex;
    while (true) {
      if (tagBuffer[tagIndex] == 0) {
        break;
      }
      tagIndex++;
    }
    
    if (hashIt) {
      tagKeyHash = XXHash.hash(tagBuffer, start, tagIndex - start);
    }
    tagKey = (long) start << 32;
    tagKey |= tagIndex;
    
    int pairHashStart = start;
    tagIndex++;
    start = tagIndex;
    while (tagIndex < tagsEnd) {
      if (tagBuffer[tagIndex] == 0) {
        break;
      }
      tagIndex++;
    }
    
    if (hashIt) {
      tagValueHash = XXHash.hash(tagBuffer, start, tagIndex - start);
      tagPairHash = XXHash.hash(tagBuffer, pairHashStart, tagIndex - pairHashStart);
    }
    
    tagValue = (long) start << 32;
    tagValue |= tagIndex;
    tagIndex++;
    return true;
  }

  @Override
  public int tagKeyStart() {
    return (int) (tagKey >> 32);
  }

  @Override
  public int tagKeyEnd() {
    return (int) tagKey;
  }

  @Override
  public int tagValueStart() {
    return (int) (tagValue >> 32);
  }

  @Override
  public int tagValueEnd() {
    return (int) tagValue;
  }

  @Override
  public int tagSetEnd() {
    return tagsEnd;
  }

  @Override
  public long metricHash() {
    return metricHash;
  }

  @Override
  public long seriesHash() {
    return seriesHash;
  }

  @Override
  public long tagsSetHash() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long tagPairHash() {
    return tagPairHash;
  }

  @Override
  public long tagKeyHash() {
    return tagKeyHash;
  }

  @Override
  public long tagValueHash() {
    return tagValueHash;
  }

  public byte[] namespaceBuffer() {
    return namespace;
  }
  
  public int namespaceStart() {
    return 0;
  }
  
  public int namespaceEnd() {
    return namespace != null ? namespace.length : 0;
  }
  
  private int findNextNewLine(final int start) {
    int printableChars = 0;
    for (int i = start + 1; i < end; i++) {
      if (buffer[i] == '\n') {
        if (printableChars >= 5) {
          // we had what could possibly be valid data.
          return i;
        }
        printableChars = 0;
      } else if (!Character.isISOControl(buffer[i]) && buffer[i] != ' ') {
        printableChars++;
      }
    }
    if (stream != null && !eos) {
      return -1;
    }
    return end;
  }
  
  private int findNextChar(int i) {
    for (; i < end; i++) {
      if (!Character.isISOControl((char) buffer[i]) && !(buffer[i] == ' ')) {
        return i;
      }
    }
    return end;
  }
  
  boolean processLine() {
    fieldIndex = valueIndex = tagsEnd = tagIndex = 0;
    boolean has_quote = false;
    boolean escaped_char = false;
    int idx = lineStart;
    if (buffer[idx] == '"' || buffer[idx] == '\'') {
      has_quote = true;
      idx++;
    }
    
    int i = lineStart;
    for (; i < lineEnd; i++) {
      if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
        // not part of spec but since we're trimming quotes, we can accept escaped
        // quotes.
        if (i -1 >= offset) {
          if (buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
        }
        break;
      } else if (!has_quote && (buffer[i] == ',' || buffer[i] == ' ' || buffer[i] == '\t')) {
        if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ')) {
          if (buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
        }
        break;
      }
    }
    
    if (metric_buffer.length < i - idx + 1) {
      metric_buffer = new byte[metric_buffer.length * 2];
    }
    
    if (escaped_char) {
      measurementIndex = 0;
      for (int x = idx; x < i; x++) {
        if (buffer[x] == '\\' && (
            buffer[x + 1] == ' ' ||
            buffer[x + 1] == '"' ||
            buffer[x + 1] == ',' ||
            buffer[x + 1] == '\'')) {
          continue;
        }
        metric_buffer[measurementIndex++] = buffer[x];
      }
      metric_buffer[measurementIndex++] = '.';
      escaped_char = false;
    } else {
      System.arraycopy(buffer, idx, metric_buffer, 0, i - idx);
      metric_buffer[i - idx] = '.';
      measurementIndex = i - idx + 1;
    }
    metricBytes = measurementIndex;
    idx = i;
    
    // now at tags, possibly
    if (has_quote) {
      idx++;
      has_quote = false;
    }
    
    if (idx >= lineEnd) {
      return false;
    }
    if (buffer[idx] == ',') {
      idx++;
      // ---------------------- TAGS ------------------
      // parse tags!
      int matched = 0;
      while (true) {
        if (idx >= lineEnd) {
          return false;
        }
        // TODO - sort!
        if (buffer[idx] == '"' || buffer[idx] == '\'') {
          has_quote = true;
          idx++;
        }
        
        i = idx;
        for (; i < lineEnd; i++) {
          if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
            // not part of spec but since we're trimming quotes, we can accept escaped
            // quotes.
            if (i -1 >= offset) {
              if (buffer[i - 1] == '\\') {
                escaped_char = true;
                continue;
              }
            }
            break;
          } else if (!has_quote && (buffer[i] == '=' || buffer[i] == ',' || buffer[i] == ' ' || buffer[i] == '\t')) {
            if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ' || buffer[i] == '=')) {
              if (buffer[i - 1] == '\\') {
                escaped_char = true;
                continue;
              }
            }
            break;
          }
        }
        
        // copy key or value
        if (tagsEnd + (i - idx) + 1 >= tagBuffer.length) {
          growTagBuffer();
        }
        
        if (i > idx) {
          if (escaped_char) {
            for (int x = idx; x < i; x++) {
              if (buffer[x] == '\\' && (
                  buffer[x + 1] == '=' ||
                  buffer[x + 1] == ' ' ||
                  buffer[x + 1] == '"' ||
                  buffer[x + 1] == ',' ||
                  buffer[x + 1] == '\'')) {
                continue;
              }
              tagBuffer[tagsEnd++] = buffer[x];
            }
            escaped_char = false;
          } else {
//            System.out.println(" COPYING: [" + new String(buffer, idx, i - idx) + "]  " + idx + " to " + i);
            System.arraycopy(buffer, idx, tagBuffer, tagsEnd, i - idx);
            tagsEnd += (i - idx);
          }
          tagBuffer[tagsEnd++] = 0;
          idx = i;
          if (++matched % 2 == 0) {
            tagsCount++;
          }
        }
        
        if (has_quote) {
          idx++;
          has_quote = false;
        }
        
        if (idx >= lineEnd) {
          return false;
        }
        if (buffer[idx] == '=' || buffer[idx] == ',') {
          idx++;
          continue;
        }
        
        if (buffer[idx] == ' ' || buffer[idx] == '\t') {
          // end of tags;
          break;
        }
      }
      
      if (matched % 2 != 0) {
        return false;
      }
      // done with tags!
      // TODO - sort
      if (hashIt) {
        tagSetHash = XXHash.hash(tagBuffer, 0, tagsEnd);
      }
    } else {
      // TODO we don't have tags. BUT WE NEED EM!
      //throw new IllegalStateException("We need tags!!");
      tagsCount = 0;
    }
    
    // skip to field(s)
    while (buffer[idx] == ' ' || buffer[idx] == '\t') {
      idx++;
    }
    escaped_char = false;
    
    // now at a field
    while (true) {
      // starting at fieldname
      if (buffer[idx] == '"' || buffer[idx] == '\'') {
        has_quote = true;
        idx++;
      }
      
      i = idx;
      for (; i < lineEnd; i++) {
        if (has_quote && buffer[i] == '"' || buffer[i] == '\'') {
          if (i -1 >= offset) {
            if (buffer[i - 1] == '\\') {
              escaped_char = true;
              continue;
            }
          }
          break;
        } else if (!has_quote && (buffer[i] == ',' || buffer[i] == ' ')) {
          if (i -1 >= offset && (buffer[i] == ',' || buffer[i] == ' ')) {
            if (buffer[i - 1] == '\\') {
              escaped_char = true;
              continue;
            }
          }
        } else if (!has_quote && buffer[i] == '=') {
          if (i -1 >= offset && buffer[i] == '=' && buffer[i - 1] == '\\') {
            escaped_char = true;
            continue;
          }
          break;
        }
      }
      
      long f;
      if (escaped_char) {
        f = (long) (idx | escaped_mask) << 32;
        System.out.println("________ ESCAPED! ");
      } else {
        f = (long) idx << 32;
      }
      f |= i;
      if (fieldIndex + 1 >= fieldIndices.length) {
        long[] temp = new long[fieldIndices.length * 2];
        System.arraycopy(fieldIndices, 0, temp, 0, fieldIndex);
        fieldIndices = temp;
      }
      fieldIndices[fieldIndex++] = f;
//      System.out.println("***** F: "+ new String(buffer, idx, i - idx) + "  " + idx + " => " + i);
      
      if (fieldIndex == 1) {
        // TODO - copy into metric buffer
        appendField(escaped_char ? idx | escaped_mask : idx, i);
      }
      escaped_char = false;
      idx = i;
      if (has_quote) {
        idx += 2;
        has_quote = false;
      } else {
        idx++;
      }
      
      if (idx >= lineEnd) {
        return false;
      }
      
      // value
      if (buffer[idx] == '"' || buffer[idx] == '\'') {
        // WARNING: We don't handle strings at this time so we skip it.
        has_quote = true;
        idx++;
      }
      
      i = idx;
      for (; i < lineEnd; i++) {
        if (buffer[i] == ',' || buffer[i] == ' ' || buffer[i] == '\t') {
          break;
        }
      }
      
      if (has_quote) {
        has_quote = false;
        fieldIndex--;
        idx = i + 1;
      } else if (!parseValue(idx, i, valueIndex == 0)) {
        fieldIndex--;
        idx = i;
      } else {
        long v = (long) idx << 32;
        v |= i;
        if (valueIndex + 1 >= valueIndices.length) {
          long[] temp = new long[valueIndices.length * 2];
          System.arraycopy(valueIndices, 0, temp, 0, valueIndex);
          valueIndices = temp;
        }
        valueIndices[valueIndex++] = v;
//        System.out.println("***** v: "+new String(buffer, idx, i - idx));
        idx = i;
      }
      
      if (i >= lineEnd) {
        idx = lineEnd;
        break;
      }
      
      if (buffer[i] == ',') {
        idx++;
        continue;
      }
      idx++;
      break; // done with values
    }
    
    // consume empty space
    while (idx < lineEnd && (buffer[idx] == ' ' || buffer[idx] == '\t')) {
      idx++;
    }
    
    if (idx >= lineEnd) {
      // no timestamp!!! use current time
      System.out.println("------- no timestamp?");
      timestamp = DateTime.nanoTime();
    } else {
      // now the timestamp if it's there.
      //i = lineEnd;
      i = idx;
      for (; i < lineEnd; i++) {
        if (Character.isISOControl(buffer[i]) || buffer[i] == '\n' || buffer[i] == ' ' || buffer[i] == '\t') {
          break;
        }
      }
      
      if (idx < i) {
        if (Parsing.parseLong(buffer, idx, i, temp_long)) {
          timestamp = temp_long[0];
        } else {
          // WTF?
          return false;
        }
      }
      idx = i + 1;
    }
    
    return fieldIndex > 0;
  }

  void appendField(final int start, final int end) {
    boolean escaped = (start & escaped_mask) != 0;

    int s = start & UNescaped_mask;
    //System.out.println("COPYING    [" + escaped + "]   NEW STRING: " + new String(buffer, s, end - s));
    if (measurementIndex + (end - s) >= metric_buffer.length) {
      System.out.println("********* GROWING metric buffer.");
      byte[] temp = new byte[metric_buffer.length * 2];
      System.arraycopy(metric_buffer, 0, temp, 0, measurementIndex);
      metric_buffer = temp;
    }
    
    if (escaped) {
      int idx = measurementIndex;
      for (int x = s; x < end; x++) {
        if (buffer[x] == '\\' && (
            buffer[x + 1] == '=' ||
            buffer[x + 1] == ' ' ||
            buffer[x + 1] == '"' ||
            buffer[x + 1] == ',' ||
            buffer[x + 1] == '\'')) {
          continue;
        }
        System.out.println("         CPYING " + x);
        metric_buffer[idx++] = buffer[x];
      }
      metricBytes = idx;
    } else {
      System.arraycopy(buffer, s, metric_buffer, measurementIndex, end - s);
      for (int i = s; i < end; i++) {
        if (buffer[i] == 0) {
          System.out.println("**************************** WTF? Original buffer had a null in our read at " + i + "! " + s + " " + end);
        }
      }
      metricBytes = measurementIndex + (end - s);
    }
    
    String s2 = new String(metric_buffer, 0, metricBytes, Const.UTF8_CHARSET);
    byte[] slice = Arrays.copyOf(metric_buffer, metricBytes);
    for (int i = 0; i < slice.length; i++) {
      if (slice[i] == 0) {
        System.out.println("!!!!!!!!!!! BAD {" + s2 + "} " + Arrays.toString(slice));
        System.exit(1);
      }
    }
    //System.out.println("NEW METRIC    [" + escaped + "]   NEW STRING: {" + s2 + "} " + Arrays.toString(slice));
    
    if (hashIt) {
      metricHash = XXHash.hash(metric_buffer, 0, metricBytes);
      seriesHash = XXHash.combineHashes(metricHash, tagSetHash);
    }
  }
  
  boolean parseValue(final int start, int end, final boolean set) {
    //System.out.println("       ST: " + start + " end " + end);
    // boolean to 1 or 0
    if (buffer[start] == 't' || buffer[start] == 'T') {
      if (set) {
        valueFormat = ValueFormat.INTEGER;
        long_value[0] = 1;
      }
      return true;
    } else if (buffer[start] == 'f' || buffer[start] == 'F') {
      if (set) {
        valueFormat = ValueFormat.INTEGER;
        long_value[0] = 0;
      }
      return true;
    } else if (buffer[end - 1] == 'i') {
      // long!
      if (set) {
        valueFormat = ValueFormat.INTEGER;
      }
      return Parsing.parseLong(buffer, start, end - 1, set ? long_value : temp_long);
    } else {
      if (buffer[end - 1] == 'u') {
        // parse as double for now
        end--;
      }
      
      if (set) {
        valueFormat = ValueFormat.DOUBLE;
      }
//      System.out.println(" PARSING NUMBER: [" + new String(buffer, start, end - start) + "]");
      return Parsing.parseDouble(buffer, start, end, set ? double_value : temp_double);
    }
  }
  
  void growTagBuffer() {
    byte[] temp = new byte[tagBuffer.length * 2];
    System.arraycopy(tagBuffer, 0, temp, 0, tagsEnd);
    tagBuffer = temp;
  }
  
}

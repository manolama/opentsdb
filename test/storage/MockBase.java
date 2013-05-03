// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.DatatypeConverter;

import net.opentsdb.core.TSDB;
import net.opentsdb.tree.Branch;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.stumbleupon.async.Deferred;

/**
 * Mock HBase implementation useful in testing calls to and from storage with
 * actual pretend data. The underlying data store is just a simple tree map 
 * with a hash map of byte arrays. The keys are all Strings (since you can't 
 * store bytes[] as a key) and qualifiers are converted to and from via ASCII.
 * <p>
 * It's not a perfect mock but is useful for the majority of unit tests. Gets,
 * puts, cas, deletes and scans are currently supported. See notes for each
 * inner class below about what does and doesn't work.
 * <p>
 * <b>Note:</b> At this time, the implementation does not support multiple 
 * column families since almost all unit tests for OpenTSDB only work with one
 * CF at a time. Also, we only have the one table for now.
 * <p>
 * <b>Warning:</b> To use this class, you need to prepare the classes for testing
 * with the @PrepareForTest annotation. The classes you need to prepare are:
 * <ul><li>TSDB</li>
 * <li>HBaseClient</li>
 * <li>GetRequest</li>
 * <li>PutRequest</li>
 * <li>KeyValue</li>
 * <li>Scanner</li>
 * <li>DeleteRequest</li></ul>
 */
public final class MockBase {
  private static final Charset ASCII = Charset.forName("ISO-8859-1");
  private TSDB tsdb = mock(TSDB.class);
  private HBaseClient client = mock(HBaseClient.class);
  private TreeMap<String, HashMap<String, byte[]>> storage = 
    new TreeMap<String, HashMap<String, byte[]>>();
  private HashSet<Integer> used_scanners = new HashSet<Integer>(2);

  private MockScanner local_scanner;
  private Scanner current_scanner;
  
  /**
   * Initializes the storage class and enables mocks of the various
   * @param default_get
   * @param default_put
   * @param default_delete
   * @param default_scan
   * @return
   */
  public MockBase(
      final boolean default_get, 
      final boolean default_put,
      final boolean default_delete,
      final boolean default_scan) {
    
    when(tsdb.getClient()).thenReturn(client);
    when(tsdb.uidTable()).thenReturn("tsdb-uid".getBytes(ASCII));
    when(tsdb.dataTable()).thenReturn("tsdb".getBytes(ASCII));
  
    // Default get answer will return one or more columns from the requested row
    if (default_get) {
      when(client.get((GetRequest)any())).thenAnswer(new MockGet());
    }
    
    // Default put answer will store the given values in the proper location.
    if (default_put) {
      when(client.put((PutRequest)any())).thenAnswer(new MockPut());
      when(client.compareAndSet((PutRequest)any(), (byte[])any()))
        .thenAnswer(new MockCAS());
    }

    if (default_delete) {
      when(client.delete((DeleteRequest)any())).thenAnswer(new MockDelete());
    }
    
    if (default_scan) {
      current_scanner = mock(Scanner.class);
      local_scanner = new MockScanner(current_scanner);
//      local_scanner.captureSetters(current_scanner);
//      
      when(client.newScanner((byte[]) any())).thenAnswer(new Answer<Scanner>() {

        @Override
        public Scanner answer(InvocationOnMock arg0) throws Throwable {
          System.out.println("Asked for a new scanner: " + local_scanner.hashCode());
//          last_scanner = current_scanner;
//          current_scanner = mock(Scanner.class);;
//
//          local_scanner = new MockScanner();
//          local_scanner.captureSetters();
//          System.out.println("Rotating scanners. Old : " + 
//              last_scanner.hashCode() + "  new: "  current_scanner.hashCode());
          if (used_scanners.contains(current_scanner.hashCode())) {
            
            current_scanner = mock(Scanner.class);
            local_scanner = new MockScanner(current_scanner);
            //local_scanner.captureSetters(current_scanner);
            System.out.println("Current scanner has been used, issuing a new one: " + local_scanner.hashCode());
          }
          when(current_scanner.nextRows()).thenAnswer(local_scanner);
          return current_scanner;
        }
        
      });      

    }
  }

  public void addColumn(final byte[] key, final byte[] qualifier, 
      final byte[] value) {
    if (!storage.containsKey(bytesToString(key))) {
      storage.put(bytesToString(key), new HashMap<String, byte[]>(1));
    }
    storage.get(bytesToString(key)).put(bytesToString(qualifier), value);
  }
  
  public int numRows() {
    return storage.size();
  }
  
  public int numColumns(final byte[] key) {
    if (!storage.containsKey(bytesToString(key))) {
      return -1;
    }
    return storage.get(bytesToString(key)).size();
  }

  public byte[] getColumn (final byte[] key, final byte[] column) {
    if (!storage.containsKey(bytesToString(key))) {
      return null;
    }
    return storage.get(bytesToString(key)).get(bytesToString(column));
  }
  
  public TSDB getTSDB() {
    return tsdb;
  }
  
  public void flushStorage() {
    storage.clear();
  }
  
  public void flushRow(final byte[] key) {
    storage.remove(bytesToString(key));
  }
  
  public void dumpToSystemOut(final boolean qualifier_ascii) {
    if (storage.isEmpty()) {
      System.out.println("Empty");
      return;
    }
    
    for (Map.Entry<String, HashMap<String, byte[]>> row : storage.entrySet()) {
      System.out.println("Row: " + row.getKey());
      
      for (Map.Entry<String, byte[]> column : row.getValue().entrySet()) {
        System.out.println("  Qualifier: " + (qualifier_ascii ?
            "\"" + new String(stringToBytes(column.getKey()), ASCII) + "\""
            : column.getKey()));
        System.out.println("    Value: " + new String(column.getValue(), ASCII));
      }
    }
  }
  
  public static String bytesToString(final byte[] bytes) {
    return DatatypeConverter.printHexBinary(bytes);
  }
  
  public static byte[] stringToBytes(final String bytes) {
    return DatatypeConverter.parseHexBinary(bytes);
  }
  
  public static Charset ASCII() {
    return ASCII;
  }
  
  private class MockGet implements Answer<Deferred<ArrayList<KeyValue>>> {
    @Override
    public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
        throws Throwable {
      final Object[] args = invocation.getArguments();
      final GetRequest get = (GetRequest)args[0];
      final String key = Branch.idToString(get.key());
      final HashMap<String, byte[]> row = storage.get(key);
      
      if (row == null) {
        return Deferred.fromResult((ArrayList<KeyValue>)null);
      } if (get.qualifiers() == null || get.qualifiers().length == 0) { 

        // return all columns from the given row
        final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(row.size());
        for (Map.Entry<String, byte[]> entry : row.entrySet()) {
          KeyValue kv = mock(KeyValue.class);
          when(kv.value()).thenReturn(entry.getValue());
          when(kv.qualifier()).thenReturn(stringToBytes(entry.getKey()));
          when(kv.key()).thenReturn(get.key());
          kvs.add(kv);
        }
        return Deferred.fromResult(kvs);
        
      } else {
        
        final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(
            get.qualifiers().length);
        
        for (byte[] q : get.qualifiers()) {
          final String qualifier = bytesToString(q);
          if (!row.containsKey(qualifier)) {
            continue;
          }

          KeyValue kv = mock(KeyValue.class);
          when(kv.value()).thenReturn(row.get(qualifier));
          when(kv.qualifier()).thenReturn(stringToBytes(qualifier));
          when(kv.key()).thenReturn(get.key());
          kvs.add(kv);
        }
        
        if (kvs.size() < 1) {
          return Deferred.fromResult((ArrayList<KeyValue>)null);
        }
        return Deferred.fromResult(kvs);
      }
    }
  }
  
  private class MockPut implements Answer<Deferred<Boolean>> {
    @Override
    public Deferred<Boolean> answer(final InvocationOnMock invocation) 
      throws Throwable {
      final Object[] args = invocation.getArguments();
      final PutRequest put = (PutRequest)args[0];
      final String key = Branch.idToString(put.key());
      
      HashMap<String, byte[]> column = storage.get(key);
      if (column == null) {
        column = new HashMap<String, byte[]>();
        storage.put(key, column);
      }
      
      for (int i = 0; i < put.qualifiers().length; i++) {
        column.put(bytesToString(put.qualifiers()[i]), put.values()[i]);
      }
      
      return Deferred.fromResult(true);
    }
  }
  
  private class MockCAS implements Answer<Deferred<Boolean>> {
    @Override
    public Deferred<Boolean> answer(final InvocationOnMock invocation) 
      throws Throwable {
      final Object[] args = invocation.getArguments();
      final PutRequest put = (PutRequest)args[0];
      final byte[] expected = (byte[])args[1];
      final String key = Branch.idToString(put.key());
      
      HashMap<String, byte[]> column = storage.get(key);
      if (column == null) {
        if (expected != null && expected.length > 0) {
          return Deferred.fromResult(false);
        }
        
        column = new HashMap<String, byte[]>();
        storage.put(key, column);
      }
      
      // CAS can only operate on one cell, so if the put request has more than 
      // one, we ignore any but the first
      final byte[] stored = column.get(bytesToString(put.qualifiers()[0])); 
      if (stored == null && (expected != null && expected.length > 0)) {
        return Deferred.fromResult(false);
      }
      if (stored != null && (expected == null || expected.length < 1)) {
        return Deferred.fromResult(false);
      }
      if (stored != null && expected != null && 
          Bytes.memcmp(stored, expected) != 0) {
        return Deferred.fromResult(false);
      }
      
      // passed CAS!
      column.put(bytesToString(put.qualifiers()[0]), put.value());
      return Deferred.fromResult(true);
    }
  }
  
  private class MockDelete implements Answer<Deferred<Object>> {
    
    @Override
    public Deferred<Object> answer(InvocationOnMock invocation)
        throws Throwable {
      final Object[] args = invocation.getArguments();
      final DeleteRequest delete = (DeleteRequest)args[0];
      final String key = Branch.idToString(delete.key());
      
      if (!storage.containsKey(key)) {
        return Deferred.fromResult(null);
      }
      
      // if no qualifiers, then delete the row
      if (delete.qualifiers() == null) {
        storage.remove(key);
        return Deferred.fromResult(new Object());
      }
      
      HashMap<String, byte[]> column = storage.get(key);
      final byte[][] qualfiers = delete.qualifiers();       
      
      for (byte[] qualifier : qualfiers) {
        final String q = bytesToString(qualifier);
        if (!column.containsKey(q)) {
          continue;
        }
        column.remove(q);
      }
      
      // if all columns were deleted, wipe the row
      if (column.isEmpty()) {
        storage.remove(key);
      }
      return Deferred.fromResult(new Object());
    }
    
  }
  
  private class MockScanner implements 
    Answer<Deferred<ArrayList<ArrayList<KeyValue>>>> {
    
    private String start = null;
    private String stop = null;
    private HashSet<String> scnr_qualifiers = null;
    private String regex = null;
    
    public MockScanner(final Scanner mock_scanner) {

      // capture the scanner fields when set
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          regex = (String)args[0];
          return null;
        }      
      }).when(mock_scanner).setKeyRegexp(anyString());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          start = bytesToString((byte[])args[0]);
          return null;
        }      
      }).when(mock_scanner).setStartKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          stop = bytesToString((byte[])args[0]);
          return null;
        }      
      }).when(mock_scanner).setStopKey((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          scnr_qualifiers = new HashSet<String>(1);
          scnr_qualifiers.add(bytesToString((byte[])args[0]));
          return null;
        }      
      }).when(mock_scanner).setQualifier((byte[])any());
      
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          final Object[] args = invocation.getArguments();
          final byte[][] qualifiers = (byte[][])args[0];
          scnr_qualifiers = new HashSet<String>(qualifiers.length);
          for (byte[] qualifier : qualifiers) {
            scnr_qualifiers.add(bytesToString(qualifier));
          }
          return null;
        }      
      }).when(mock_scanner).setQualifiers((byte[][])any());
      
    }
    
    @Override
    public Deferred<ArrayList<ArrayList<KeyValue>>> answer(
        final InvocationOnMock invocation) throws Throwable {
      
      System.out.println("In scanner answer");
      if (used_scanners.contains(current_scanner.hashCode())) {
        System.out.println("Done w scanners");
        return Deferred.fromResult(null);
      }
      used_scanners.add(current_scanner.hashCode());
      
      Pattern pattern = null;
      if (regex != null && !regex.isEmpty()) {
        try {
          Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
          e.printStackTrace();
        }
        System.out.println("Compiled a scanner regex");
      }
      
      // return all matches
      ArrayList<ArrayList<KeyValue>> results = 
        new ArrayList<ArrayList<KeyValue>>();
      System.out.println("HERERERE");
      System.out.println("ST: " + start + "  Ed: " + stop);
      for (Map.Entry<String, HashMap<String, byte[]>> row : storage.entrySet()) {
        
        // if it's before the start row, after the end row or doesn't
        // match the given regex, continue on to the next row
        if (start != null) {
          if (row.getKey().compareTo(start) < 0) {
            System.out.println("start diff " + row.getKey().compareTo(start));
            continue;
          }
        }
        if (stop != null) {

          if (row.getKey().compareTo(stop) > 0) {
            System.out.println("End diff " + row.getKey().compareTo(stop));
            continue;
          }
        }
        if (pattern != null && !pattern.matcher(row.getKey()).find()) {
          System.out.println("Didn't match pattern: " + row.getKey());
          continue;
        }
        
        // loop on the columns
        final ArrayList<KeyValue> kvs = 
          new ArrayList<KeyValue>(row.getValue().size());
        for (Map.Entry<String, byte[]> entry : row.getValue().entrySet()) {
          
          // if the qualifier isn't in the set, continue
          if (scnr_qualifiers != null && 
              !scnr_qualifiers.contains(entry.getKey())) {
            continue;
          }
          
          KeyValue kv = mock(KeyValue.class);
          when(kv.key()).thenReturn(stringToBytes(row.getKey()));
          when(kv.value()).thenReturn(entry.getValue());
          when(kv.qualifier()).thenReturn(stringToBytes(entry.getKey()));
          kvs.add(kv);
        }
        
        if (!kvs.isEmpty()) {
          results.add(kvs);
        }
      }
      
      if (results.isEmpty()) {
        return Deferred.fromResult(null);
      }
      return Deferred.fromResult(results);
    }
  }
}

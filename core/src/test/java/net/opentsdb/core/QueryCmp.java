package net.opentsdb.core;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;

import net.opentsdb.utils.JSON;

public class QueryCmp {

  public static void main(final String[] args) throws Exception {
    String filename = "/Users/clarsen/Documents/opentsdb/scratch/tsd1.json";
    File file = new File(filename);
    FileInputStream fis = new FileInputStream(file);
    byte[] data = new byte[(int) file.length()];
    fis.read(data);
    fis.close();
    
    List<TSDEntry> entries = JSON.parseToObject(data, QUERY_RESULTS);
    Map<Integer, TSDEntry> mapped = Maps.newHashMap();
    for (final TSDEntry e : entries) {
      mapped.put(e.hashCode(), e);
    }
    
    filename = "/Users/clarsen/Documents/opentsdb/scratch/tsd2.json";
    file = new File(filename);
    fis = new FileInputStream(file);
    data = new byte[(int) file.length()];
    fis.read(data);
    fis.close();
    
    List<TSDEntry> entries2 = JSON.parseToObject(data, QUERY_RESULTS);
    
    for (final TSDEntry b : entries2) {
      TSDEntry a = mapped.get(b.hashCode());
      if (a == null) {
        System.out.println("WTF? Nothing in the second result for: " + b);
      }
      
      int cnt = 0;
      int missing = 0;
      int diff = 0;
      for (final Entry<Long, Double> e : a.dps.entrySet()) {
        Double other = b.dps.get(e.getKey());
        if (other == null) {
          System.out.println("  Other was missing one....");
          missing++;
        } else {
          double v1 = (Double) e.getValue();
          double v2 = (Double) other;
          double delta = Math.abs(v1 - v2);
          if (delta > 0.000001D) {
            System.out.println("  A[" + v1 + "]  B[" + v2 + "]  Delta[" + delta + "]  at " + e.getKey());
            diff++;
          }
        }
        cnt++;
      }
      
      if (missing > 0) {
        System.out.println("B was missing " + missing + " data points for: " + a);
      } else if (diff > 0) {
        System.out.println("B had " + diff + " different data points for: " + a);
      } else {
        System.out.println(a + " was OK!");
      }
    }
    
    System.out.println("\nAll Done.\n");
  }
  
  static TypeReference<List<TSDEntry>> QUERY_RESULTS = new TypeReference<List<TSDEntry>>() { };
  static class TSDEntry {
    public String metric;
    public Map<String, String> tags;
    public List<String> aggregateTags;
    public Map<Long, Double> dps;
    
    @Override
    public String toString() {
      return new StringBuilder()
          .append(metric)
          .append(" ")
          .append(tags)
          .append(" ")
          .append(aggregateTags)
          .toString();
    }
    
    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (o == this) {
        return true;
      }
      if (!(o instanceof TSDEntry)) {
        return false;
      }
      
      TSDEntry other = (TSDEntry) o;
      if (!other.metric.equals(metric)) {
        return false;
      }
      for (final Entry<String, String> e : tags.entrySet()) {
        if (!other.tags.containsKey(e.getKey()) || 
            !other.tags.get(e.getKey()).equals(e.getValue())) {
          return false;
        }
      }
      for (final String tagk : aggregateTags) {
        if (!other.aggregateTags.contains(tagk)) {
          return false;
        }
      }
      return true;
    }
    
    @Override
    public int hashCode() {
      StringBuilder buf = new StringBuilder()
          .append(metric);
      
      TreeMap<String, String> t = new TreeMap<String, String>(tags);
      for (final Entry<String, String> e : t.entrySet()) {
        buf.append(e.getKey())
           .append(e.getValue());
      }
      Collections.sort(aggregateTags);
      for (final String tag : aggregateTags) {
        buf.append(tag);
      }
      
      return Const.HASH_FUNCTION().hashString(buf.toString(), Const.UTF8_CHARSET).asInt();
    }
  }
}

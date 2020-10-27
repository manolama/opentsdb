package net.opentsdb.data.influx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.common.Const;
import net.opentsdb.data.LowLevelMetric.ValueFormat;
import net.opentsdb.data.influx.Influx2;
import net.opentsdb.utils.DateTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class })
public class TestInflux2 {
  
  @Test
  public void singleLineTagsOneFieldTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldTimestampBlankspace() throws Exception {
    String msg = " sys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200  ";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldNoTimestamp() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime()).thenReturn(1234123456789012345L);
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789012345L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldNoTimestampBlankspace() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime()).thenReturn(1234123456789012345L);
    String msg = "  sys.if,tagKey=Value,tagk2=Value2 in=10.24  ";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789012345L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineNoTagsTimestamp() throws Exception {
    String msg = "sys.if in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineNoTagsNoTimestamp() throws Exception {
    PowerMockito.mockStatic(DateTime.class);
    when(DateTime.nanoTime()).thenReturn(1234123456789012345L);
    String msg = "sys.if in=10.24";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1234123456789012345L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsMultipleFieldsTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i,err=.05 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    // next field
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricEnd() - parser.metricStart(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.intValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.err", new String(parser.metricBuffer(), 
        parser.metricStart(), 
        parser.metricEnd() - parser.metricStart(), 
        Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    //assertEquals(0.05, parser.doubleValue(), 0.001); TODO - fix parsing
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineMalformed() throws Exception {
    // no value for field
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no equals after field name.
    msg = "sys.if,tagKey=Value,tagk2=Value2 in";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no fields
    msg = "sys.if,tagKey=Value,tagk2=Value2";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // no fields... but a timestamp??
    msg = "sys.if,tagKey=Value,tagk2=Value2 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ends with tag value missing
    msg = "sys.if,tagKey=Value,tagk2=";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // whoops nulled tag value
    msg = "sys.if,tagKey=Value,tagk2= in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ends with tag value missing
    msg = "sys.if,tagKey=Value,tagk2";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // whoops nulled tag value
    msg = "sys.if,tagKey=Value,tagk2 in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // ok, yeah, missing a key. Ew
    msg = "sys.if,tagKey=Value,=value2 in=10.24 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // just a measurement and timestamp
    msg = "sys.if 1465839830100400200";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // just a measurement?
    msg = "sys.if";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
    
    // shoulda beena comment
    msg = "This is a measurement from our Internet Scale app!";
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineTagsOneFieldTimestampTabs() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys.if,tagKey=Value,tagk2=Value2\tin=10.24\t1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedMetric() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys\\,\\ if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys, if.in", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricEnd() - parser.metricStart(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagKeys() throws Exception {
    String msg = "sys.if,tag\\ Key=Value,tag\\=k\\,2=Value2 in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tag Key", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyEnd() - parser.tagKeyStart(),
                                      Const.UTF8_CHARSET));
    assertEquals("Value", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tag=k,2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyEnd() - parser.tagKeyStart(),
                                     Const.UTF8_CHARSET));
    assertEquals("Value2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagValues() throws Exception {
    String msg = "sys.if,tagKey=Va\\ lue,tagk2=Va\\=lu\\,e2 in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tagKey", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyEnd() - parser.tagKeyStart(),
                                      Const.UTF8_CHARSET));
    assertEquals("Va lue", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tagk2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyEnd() - parser.tagKeyStart(),
                                     Const.UTF8_CHARSET));
    assertEquals("Va=lu,e2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedTagKeysAndValues() throws Exception {
    String msg = "sys.if,tag\\ Key=V\\a\\ lue,ta\\=gk\\,2=Va\\=lu\\,e2 in=10.24 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTrue(parser.advanceTagPair());
    assertEquals("tag Key", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyEnd() - parser.tagKeyStart(),
                                      Const.UTF8_CHARSET));
    assertEquals("V\\a lue", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("ta=gk,2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyEnd() - parser.tagKeyStart(),
                                     Const.UTF8_CHARSET));
    assertEquals("Va=lu,e2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
    assertFalse(parser.advance());
  }
  
  @Test
  public void singleLineEscapedField() throws Exception {
    // not to spec but hey, let's be kind.
    String msg = "sys.if,tagKey=Value,tagk2=Value2 i\\,\\ n=10.24,te\\=st=42 1465839830100400200";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.i, n", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricEnd() - parser.metricStart(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertTrue(parser.advance());
    
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.te=st", new String(parser.metricBuffer(), 
                                          parser.metricStart(), 
                                          parser.metricEnd() - parser.metricStart(), 
                                          Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(42, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void newLinesBeforeAndAfter() throws Exception {
    String msg = "\n\nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void newLinesBeforeAndAfterWithBlankSpace() throws Exception {
    String msg = "  \n  \nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n  \n";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void CommentsBeforeAndAfterWithBlankSpace() throws Exception {
    String msg = "# ignore this line\nsys.if,tagKey=Value,tagk2=Value2 in=10.24 1465839830100400200\n# and this";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    assertFalse(parser.advance());
  }
  
  @Test
  public void twoLinesTagsTwoFieldsTimestamp() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i 1465839830100400200\n"
        + "sys.cpu,tagKey=Value,tagk2=Value2 sys=-1.234456e+78,user=41 1465839830100000000\n";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.intValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100000000L, parser.timestamp());
    assertEquals("sys.cpu.sys", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(-1.234456e+78, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100000000L, parser.timestamp());
    assertEquals("sys.cpu.user", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(41, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  @Test
  public void twoLinesTagsTwoFieldsTimestampCommentInBetween() throws Exception {
    String msg = "sys.if,tagKey=Value,tagk2=Value2 in=10.24,out=42i 1465839830100400200\n"
        + "# comment\n"
        + "sys.cpu,tagKey=Value,tagk2=Value2 sys=-1.234456e+78,user=41 1465839830100000000\n";
    Influx2 parser = new Influx2();
    parser.setBuffer(msg.getBytes(Const.UTF8_CHARSET));
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.in", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(10.24, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100400200L, parser.timestamp());
    assertEquals("sys.if.out", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.INTEGER, parser.valueFormat());
    assertEquals(42, parser.intValue());
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100000000L, parser.timestamp());
    assertEquals("sys.cpu.sys", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(-1.234456e+78, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertTrue(parser.advance());
    assertEquals(1465839830100000000L, parser.timestamp());
    assertEquals("sys.cpu.user", new String(parser.metricBuffer(), 
                                         parser.metricStart(), 
                                         parser.metricEnd() - parser.metricStart(), 
                                         Const.UTF8_CHARSET));
    assertEquals(ValueFormat.DOUBLE, parser.valueFormat());
    assertEquals(41, parser.doubleValue(), 0.001);
    assertTags(parser);
    
    assertFalse(parser.advance());
  }
  
  //@Test
  public void foo() throws Exception {
    //  String msg = "sys.if,tagKey=tagValue,tagk2=tagv2 in=10.24,out=\"skip\" 1465839830100400200\n"
    //  + "sys.cpu,tagKey=tagValue,tagk2=tagv2 sys=-1.234456e+78,user=41 1465839830100000000\n";
    String msg = "sys.if,tagKey=tagValue,tagk2=tagv2 in=10.24,out=34i 1465839830100400200\n"
      + "# A comment\n"
      + "sys\\ c\\,pu,tagKey=tagValue,tagk2=tagv2 sys=-1.234456e+78,user=41 1465839830100000000\n";

    byte[] buf = msg.getBytes(Const.UTF8_CHARSET);
    Influx2 influx = new Influx2();
    influx.setBuffer(buf);
  
    int read = 0;
    while (influx.advance()) {
      print(influx);
      read++;
    }
    System.out.println("------------------- DONE ");
    System.out.println("------------------- READ: " + read);
//    
//    if (influx.advance()) {
//      print(influx);
//    }
//    
//    if (influx.advance()) {
//      print(influx);
//    }
//    
//    if (influx.advance()) {
//      print(influx);
//    }
    
//    System.out.println(" False? " + influx.advance());
  }
  
  void print(Influx2 influx) {
    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    //System.out.println(influx.hasNext());
    System.out.println("    Metric: " + new String(influx.metricBuffer(), influx.metricStart(), influx.metricEnd()));
    System.out.println("    Tags: " + new String(influx.tagsBuffer()));
    System.out.println("    Timestamp: " + influx.timestamp);
    System.out.println("    Type: " + influx.valueFormat());
    if (influx.valueFormat() == ValueFormat.INTEGER) {
      System.out.println("    INT: " + influx.intValue());
    } else {
      System.out.println("    DOUBLE: " + influx.doubleValue());
    }
    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
  }

  void assertTags(final Influx2 parser) {
    assertTrue(parser.advanceTagPair());
    assertEquals("tagKey", new String(parser.tagsBuffer(),
                                      parser.tagKeyStart(),
                                      parser.tagKeyEnd() - parser.tagKeyStart(),
                                      Const.UTF8_CHARSET));
    assertEquals("Value", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertTrue(parser.advanceTagPair());
    assertEquals("tagk2", new String(parser.tagsBuffer(),
                                     parser.tagKeyStart(),
                                     parser.tagKeyEnd() - parser.tagKeyStart(),
                                     Const.UTF8_CHARSET));
    assertEquals("Value2", new String(parser.tagsBuffer(),
                                     parser.tagValueStart(),
                                     parser.tagValueEnd() - parser.tagValueStart(),
                                     Const.UTF8_CHARSET));
    assertFalse(parser.advanceTagPair());
  }
}

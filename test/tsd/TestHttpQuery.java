package net.opentsdb.tsd;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Map;


import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpQuery.class })
public class TestHttpQuery {
 
  @Test
  public void getQueryString(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.get("param").get(0).equals("value"));
    assertTrue(params.get("param2").get(0).equals("value2"));
  }
  
  @Test
  public void getQueryStringEmpty(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.size() == 0);
  }
  
  @Test
  public void getQueryStringMulti(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
    assertTrue(params.size() == 1);
    assertTrue(params.get("param").size() == 3);
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryStringNULL(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    Map<String, List<String>> params = query.getQueryString();
    assertNotNull(params);
  }
  
  @Test
  public void getQueryStringParam(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryStringParam("param").equals("value"));
  }
  
  @Test
  public void getQueryStringParamNull(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertNull(query.getQueryStringParam("nothere"));
  }
  
  @Test
  public void getRequiredQueryStringParam(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getRequiredQueryStringParam("param").equals("value"));
  }
  
  @Test (expected = BadRequestException.class)
  public void getRequiredQueryStringParamMissing(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    query.getRequiredQueryStringParam("nothere");
  }
  
  @Test
  public void hasQueryStringParam(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.hasQueryStringParam("param"));
  }
  
  @Test
  public void hasQueryStringMissing(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertFalse(query.hasQueryStringParam("nothere"));
  }
  
  @Test
  public void getQueryStringParams(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    List<String> params = query.getQueryStringParams("param");
    assertNotNull(params);
    assertTrue(params.size() == 3);
  }
  
  @Test
  public void getQueryStringParamsNull(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=v1&param=v2&param=v3");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    List<String> params = query.getQueryStringParams("nothere");
    assertNull(params);
  }
  
  @Test
  public void getQueryPathA(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/api/v1/put"));
  }
  
  @Test
  public void getQueryPathB(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/"));
  }
  
  @Test (expected = NullPointerException.class)
  public void getQueryPathNull(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getQueryPath().equals("/"));
  }
  
  @Test
  public void explodePath(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/api/v1/put?param=value&param2=value2");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    final String[] path = query.explodePath();
    assertNotNull(path);
    assertTrue(path.length == 3);
    assertTrue(path[0].equals("api"));
    assertTrue(path[1].equals("v1"));
    assertTrue(path[2].equals("put"));
  }
  
  @Test
  public void explodePathEmpty(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    final String[] path = query.explodePath();
    assertNotNull(path);
    System.out.println("PAth: " + path.length);
    assertTrue(path.length == 0);
  }
  
  @Test (expected = NullPointerException.class)
  public void explodePathNull(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, null);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    @SuppressWarnings("unused")
    final String[] path = query.explodePath();
  }
  
  @Test
  public void getCharsetDefault(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-8")));
  }
  
  @Test
  public void getCharsetDefaultNoHeader(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-8")));
  }
  
  @Test
  public void getCharsetSupplied(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=UTF-16");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-16")));
  }
  
  @Test (expected = UnsupportedCharsetException.class)
  public void getCharsetInvalid(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=foobar");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getCharset().equals(Charset.forName("UTF-16")));
  }
  
  @Test
  public void getContentEncoding(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    req.addHeader("Content-Type", "text/plain; charset=UTF-16");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_16);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentDefault(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.UTF_8);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentBadEncoding(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final ChannelBuffer buf = ChannelBuffers.copiedBuffer("S\u00ED Se\u00F1or", 
        CharsetUtil.ISO_8859_1);
    req.setContent(buf);
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertFalse(query.getContent().equals("S\u00ED Se\u00F1or"));
  }
  
  @Test
  public void getContentEmpty(){
    final Channel channelMock = fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "/");
    final HttpQuery query = new HttpQuery(null, req, channelMock);
    assertTrue(query.getContent().isEmpty());
  }
  
  private static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    return chan;
  }
  
  private DefaultChannelPipeline createPipeline() {
    DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
    pipeline.addLast("requestDecoder", new HttpRequestDecoder());
    pipeline.addLast("responseEncoder", new HttpResponseEncoder());
    return pipeline;
  } 
}
